import logging
from datetime import datetime
from threading import Thread
from queue import Queue
import pandas as pd

from ibapi.common import BarData, TickerId
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

logger = logging.getLogger(__name__)


def historical_trades_row_function(bar: BarData):
    return (datetime.strptime(bar.date, "%Y%m%d %H:%M:%S"), [
        bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average
    ])


def historical_data_row_function(bar: BarData):
    return (datetime.strptime(bar.date, "%Y%m%d %H:%M:%S"), [
        bar.open, bar.high, bar.low, bar.close, bar.barCount, bar.average
    ])


class RequestWrapper(EWrapper):
    """ Wrapper that turns the callback-based IB API Wrapper into a blocking API, by collecting results into tables
    and returning the complete tables.
    """

    REQUEST_OPTIONS_HISTORICAL_TYPE = [
        "TRADES",
        "MIDPOINT",
        "BID",
        "ASK",
        "BID_ASK",
        "HISTORICAL_VOLATILITY",
        "OPTION_IMPLIED_VOLATILITY",
    ]

    REQUEST_OPTIONS_BAR_SIZE = [
        "1 sec",
        "5 secs",
        "15 secs",
        "30 secs",
        "1 min",
        "2 mins",
        "3 mins",
        "5 mins",
        "15 mins",
        "30 mins",
        "1 hour",
        "1 day",
    ]

    REQUEST_FIELDS_HISTORICAL_TRADES = ["open", "high", "low", "close", "volume", "count", "average"]
    REQUEST_FIELDS_HISTORICAL_DATA = ["open", "high", "low", "close", "count", "average"]
    REQUEST_FIELDS = dict(
        STOCK_DETAILS=[
            # from Contract
            "ticker", "exchange",
            # from ContractDetails
            "long_name", "industry", "category", "sub_category", "time_zone_id", "trading_hours", "liquid_hours"
        ],
        OPTION_PARAMS=[
            "exchange", "multiplier", "expirations", "strikes",
        ],
        OPTION_DETAILS=[
            # from Contract
            "option_ticker", "exchange", "expiration", "strike", "right", "multiplier",
        ],
        HISTORICAL_TRADES_EQUITY=REQUEST_FIELDS_HISTORICAL_TRADES,
        HISTORICAL_TRADES_OPTIONS=REQUEST_FIELDS_HISTORICAL_TRADES,
        HISTORICAL_BID_ASK_OPTIONS=["average_bid", "max_ask", "min_bid", "average_ask"],
        HISTORICAL_IV_EQUITY=REQUEST_FIELDS_HISTORICAL_DATA,
        HISTORICAL_HV_EQUITY=REQUEST_FIELDS_HISTORICAL_DATA,
    )

    REQUEST_CALLBACKS = dict(
        STOCK_DETAILS="contractDetails",
        OPTION_PARAMS="securityDefinitionOptionParameter",
        OPTION_DETAILS="contractDetails",
        HISTORICAL_TRADES_EQUITY="historicalData",
        HISTORICAL_TRADES_OPTIONS="historicalData",
        HISTORICAL_BID_ASK_OPTIONS="historicalData",
        HISTORICAL_IV_EQUITY="historicalData",
        HISTORICAL_HV_EQUITY="historicalData",
    )

    RESPONSE_ROW_FUNCTION = dict(
        STOCK_DETAILS=lambda contract_details: (contract_details.contract.conId, [
            contract_details.contract.symbol,
            contract_details.contract.exchange,
            contract_details.longName,
            contract_details.industry,
            contract_details.category,
            contract_details.subcategory,
            contract_details.timeZoneId,
            contract_details.tradingHours,
            contract_details.liquidHours,
        ]),

        OPTION_PARAMS=lambda exchange, _, __, multiplier, expirations, strikes: (None, [
            exchange,
            multiplier,
            expirations,
            strikes,
        ]),

        OPTION_DETAILS=lambda contract_details: (contract_details.contract.conId, [
            contract_details.contract.localSymbol,
            contract_details.contract.exchange,
            contract_details.contract.lastTradeDateOrContractMonth,
            contract_details.contract.strike,
            contract_details.contract.right,
            contract_details.contract.multiplier,
        ]),
        HISTORICAL_TRADES_EQUITY=historical_trades_row_function,
        HISTORICAL_TRADES_OPTIONS=historical_trades_row_function,
        HISTORICAL_BID_ASK_OPTIONS=lambda bar: (datetime.strptime(bar.date, "%Y%m%d %H:%M:%S"), [
            bar.open, bar.high, bar.low, bar.close,
        ]),
        HISTORICAL_IV_EQUITY=historical_data_row_function,
        HISTORICAL_HV_EQUITY=historical_data_row_function,
    )

    def __init__(self, timeout: int = None):
        """
        Create an EWrapper to provide blocking access to the callback-based IB API.
        :param timeout: Amount of time in seconds to wait for a response before giving up. Use None to never give up.
        """
        EWrapper.__init__(self)
        self.response_queue = Queue()
        self.timeout = timeout
        self._app = None
        self.current_request_id = 0
        self.current_request = None
        self.response_table = None
        self.thread = None

    def start_app(self, host: str, port: int, client_id: int):
        """ Start a connection ton IB TWS application in a background thread and confirm connection is successful.
        :param host: Hostname to connect to, usually 127.0.0.1
        :param port: Port to connect to, configurable and differs for live vs paper trading.
        :param client_id: Client ID setting for the TWS API
        """
        self._app = EClient(wrapper=self)
        self.current_request_id = 0
        self.current_request = "INIT"
        self.response_table = None
        self._app.connect(host, port, client_id)
        self.thread = Thread(target=self._app.run, daemon=True)
        self.thread.start()
        # connectAck will add a None to the queue during INIT
        self.response_queue.get(timeout=self.timeout)

    def stop_app(self):
        """ Disconnect from the IB TWS and wait for the background thread to end. """
        self._app.disconnect()
        self.thread.join()

    @property
    def app(self):
        """ The currently running application representing the connection to the IB TWS """
        return self._app

    def request_stock_details(self, ticker: str, **kwargs):
        """ Performs a search using the ticker and provides a table of results including
        the general information about each match.
        :param ticker: stock ticker to search

        :Keyword Arguments:
            * *exchange* (``str``) --
              Exchange to look on, i.e. "SMART"
            * *currency* (``str``) --
              Currency to report information in, i.e. "USD"
        """
        self._start_request("STOCK_DETAILS")
        contract = self._get_stock_contract(ticker, **kwargs)
        self._app.reqContractDetails(self.current_request_id, contract)
        return self.response_queue.get(timeout=self.timeout)

    def request_option_params(self, ticker: str, contract_id: int):
        """ Request options expiration and strike information about the provided stock ticker and contract_id.
        :param ticker: stock ticker with available options
        :param contract_id: contract ID of the stock with available options, returned by request_stock_details
        """
        self._start_request("OPTION_PARAMS")
        self._app.reqSecDefOptParams(self.current_request_id,
                                     ticker,
                                     "",  # Leave blank so it will return all exchange options
                                     "STK",
                                     contract_id)
        return self.response_queue.get(timeout=self.timeout)

    def request_option_chain(self, ticker: str, exchange: str, expiration: str, currency="USD"):
        """ Request a list of all the options available for a given ticker and expiration.
        :param ticker: stock ticker with available options
        :param exchange: exchange of the options contracts
        :param expiration: expiration of the options contracts, in YYYYMMDD format
        :param currency: currency to report information in
        """
        self._start_request("OPTION_DETAILS")
        # do not use _get_option_contract shortcut because we are leaving right and strike blank
        contract = Contract()
        contract.secType = "OPT"
        contract.symbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiration
        self._app.reqContractDetails(self.current_request_id, contract)
        return self.response_queue.get(timeout=self.timeout)

    def request_stock_trades_history(self, ticker: str, **kwargs):
        """ Request historical data for stock trades for the given ticker
        :param ticker: stock ticker to search

        :Keyword Arguments:
            * *exchange* (``str``) --
              Exchange to look on, i.e. "SMART"
            * *currency* (``str``) --
              Currency to report information in, i.e. "USD"
            * *duration* (``str``) --
              Amount of time to collect data for, i.e. "5 d" for five days of data.
            * *bar_size* (''str'') --
              Time interval that data is reported in, i.e. "30 mins" provides 30 minute bars
            * *query_time* (''str'') --
              End (latest, most recent) datetime of the returned historical data, in format "%Y%m%d %H:%M:%S"
            * *after_hours* (''bool'') --
              If True, data from outside normal market hours for this security are also returned.
        """
        self._start_request("HISTORICAL_TRADES_EQUITY")
        contract = self._get_stock_contract(ticker, **kwargs)
        return self._request_historical(contract, "TRADES", **kwargs)

    def request_stock_iv_history(self, ticker: str, **kwargs):
        """ Request historical data for stock implied volatility for the given ticker
        :param ticker: stock ticker to search

        :Keyword Arguments:
            * *exchange* (``str``) --
              Exchange to look on, i.e. "SMART"
            * *currency* (``str``) --
              Currency to report information in, i.e. "USD"
            * *duration* (``str``) --
              Amount of time to collect data for, i.e. "5 d" for five days of data.
            * *bar_size* (''str'') --
              Time interval that data is reported in, i.e. "30 mins" provides 30 minute bars
            * *query_time* (''str'') --
              End (latest, most recent) datetime of the returned historical data, in format "%Y%m%d %H:%M:%S"
            * *after_hours* (''bool'') --
              If True, data from outside normal market hours for this security are also returned.
        """
        self._start_request("HISTORICAL_IV_EQUITY")
        contract = self._get_stock_contract(ticker, **kwargs)
        return self._request_historical(contract, "OPTION_IMPLIED_VOLATILITY", **kwargs)

    def request_stock_hv_history(self, ticker: str, **kwargs):
        """ Request historical data for stock historical volatility for the given ticker
        :param ticker: stock ticker to search

        :Keyword Arguments:
            * *exchange* (``str``) --
              Exchange to look on, i.e. "SMART"
            * *currency* (``str``) --
              Currency to report information in, i.e. "USD"
            * *duration* (``str``) --
              Amount of time to collect data for, i.e. "5 d" for five days of data.
            * *bar_size* (''str'') --
              Time interval that data is reported in, i.e. "30 mins" provides 30 minute bars
            * *query_time* (''str'') --
              End (latest, most recent) datetime of the returned historical data, in format "%Y%m%d %H:%M:%S"
            * *after_hours* (''bool'') --
              If True, data from outside normal market hours for this security are also returned.
        """
        self._start_request("HISTORICAL_HV_EQUITY")
        contract = self._get_stock_contract(ticker, **kwargs)
        return self._request_historical(contract, "HISTORICAL_VOLATILITY", **kwargs)

    def request_option_trades_history(self, ticker: str, expiration: str, strike: float, right: str, **kwargs):
        """ Request historical data for option trades for the given options contract
        :param ticker: stock ticker with available options
        :param expiration: expiration of the options contract, in "%Y%m%d" format
        :param strike: strike price of the options contract
        :param right: "C" for call options and "P" for put options

        :Keyword Arguments:
            * *exchange* (``str``) --
              Exchange to look on, i.e. "SMART"
            * *currency* (``str``) --
              Currency to report information in, i.e. "USD"
            * *duration* (``str``) --
              Amount of time to collect data for, i.e. "5 d" for five days of data.
            * *bar_size* (''str'') --
              Time interval that data is reported in, i.e. "30 mins" provides 30 minute bars
            * *query_time* (''str'') --
              End (latest, most recent) datetime of the returned historical data, in format "%Y%m%d %H:%M:%S"
            * *after_hours* (''bool'') --
              If True, data from outside normal market hours for this security are also returned.
        """
        self._start_request("HISTORICAL_TRADES_OPTIONS")
        contract = self._get_option_contract(ticker, expiration, strike, right, **kwargs)
        return self._request_historical(contract, "TRADES", **kwargs)

    def request_option_bidask_history(self, ticker: str, expiration: str, strike: float, right: str, **kwargs):
        """ Request historical data for option bid and ask for the given options contract
        :param ticker: stock ticker with available options
        :param expiration: expiration of the options contract, in "%Y%m%d" format
        :param strike: strike price of the options contract
        :param right: "C" for call options and "P" for put options

        :Keyword Arguments:
            * *exchange* (``str``) --
              Exchange to look on, i.e. "SMART"
            * *currency* (``str``) --
              Currency to report information in, i.e. "USD"
            * *duration* (``str``) --
              Amount of time to collect data for, i.e. "5 d" for five days of data.
            * *bar_size* (''str'') --
              Time interval that data is reported in, i.e. "30 mins" provides 30 minute bars
            * *query_time* (''str'') --
              End (latest, most recent) datetime of the returned historical data, in format "%Y%m%d %H:%M:%S"
            * *after_hours* (''bool'') --
              If True, data from outside normal market hours for this security are also returned.
        """
        self._start_request("HISTORICAL_BID_ASK_OPTIONS")
        contract = self._get_option_contract(ticker, expiration, strike, right, **kwargs)
        return self._request_historical(contract, "BID_ASK", **kwargs)

    # ------------------------------------------------------------------------------------------------------------------
    # Internal helper methods
    # ------------------------------------------------------------------------------------------------------------------
    def _start_request(self, request_name):
        self.current_request_id += 1
        self.current_request = request_name
        self.response_queue = Queue()
        self.response_table = pd.DataFrame(columns=self.REQUEST_FIELDS[request_name])

    @staticmethod
    def _get_stock_contract(ticker: str, exchange="SMART", currency="USD", **_):
        contract = Contract()
        contract.secType = "STK"
        contract.localSymbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def _get_option_contract(ticker: str, expiration: str, strike: float, right: str,
                             exchange="SMART", currency="USD", **_):
        if right not in ["C", "P"]:
            raise ValueError(f"Invalid right: {right}")
        contract = Contract()
        contract.secType = "OPT"
        contract.symbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiration
        contract.strike = strike
        contract.right = right
        return contract

    def _request_historical(self, contract: Contract, data_type: str, duration="5 d", bar_size="30 mins",
                            query_time=datetime.today().strftime("%Y%m%d %H:%M:%S"), after_hours=False, **_):
        if data_type not in self.REQUEST_OPTIONS_HISTORICAL_TYPE:
            raise ValueError(f"Invalid data type '{data_type}'. Valid options: {self.REQUEST_OPTIONS_HISTORICAL_TYPE}")

        if bar_size not in self.REQUEST_OPTIONS_BAR_SIZE:
            raise ValueError(f"Invalid data type '{bar_size}'. Valid options: {self.REQUEST_OPTIONS_BAR_SIZE}")

        self._app.reqHistoricalData(reqId=self.current_request_id,
                                    contract=contract,
                                    endDateTime=query_time,
                                    durationStr=duration,
                                    barSizeSetting=bar_size,
                                    whatToShow=data_type,
                                    useRTH=0 if after_hours else 1,
                                    formatDate=1,
                                    keepUpToDate=False,
                                    chartOptions=[])
        return self.response_queue.get()

    def _handle_callback(self, callback_name, request_id, *args):
        if request_id != self.current_request_id:
            logger.error(f"Ignoring unexpected {callback_name} call from request id {request_id} "
                         f"while processing other request id {self.current_request_id}")
            return

        expected_callback_name = self.REQUEST_CALLBACKS[self.current_request]
        if expected_callback_name != callback_name:
            logger.error(f"Ignoring unexpected callback {callback_name}, expected callback {expected_callback_name}")
            return

        index, row = self.RESPONSE_ROW_FUNCTION[self.current_request](*args)
        if index is None:
            index = len(self.response_table.index)
        self.response_table.loc[index] = row

    def _handle_callback_end(self, callback_name, request_id):
        if request_id != self.current_request_id:
            logger.error(f"Ignoring unexpected {callback_name}End call from request id {request_id} "
                         f"while processing other request id {self.current_request_id}")
            return

        expected_callback_name = self.REQUEST_CALLBACKS[self.current_request]
        if expected_callback_name != callback_name:
            logger.error(f"Ignoring unexpected callback {callback_name}End, "
                         f"expected callback {expected_callback_name}End")
            return

        # deliver the data table
        self.response_queue.put(self.response_table)

    # ------------------------------------------------------------------------------------------------------------------
    # Callbacks from the IB TWS
    # ------------------------------------------------------------------------------------------------------------------

    def error(self, req_id: TickerId, error_code: int, error_string: str):
        """This event is called when there is an error with the
        communication or when TWS wants to send a message to the client."""
        logger.error(f"{error_string} (req_id:{req_id}, error_code:{error_code})")

        if 2000 <= error_code < 10000:  # non-fatal
            pass
        elif error_code == 10167:  # delayed market data instead
            pass
        else:
            logger.error("Ending response since error code is fatal")
            self.response_queue.put(self.response_table)

    def connectAck(self):
        super().connectAck()
        logger.info("Connection successful.")
        if self.current_request == "INIT":
            self.response_queue.put(None)

    def contractDetails(self, request_id: int, *args):
        super().contractDetails(request_id, *args)
        self._handle_callback("contractDetails", request_id, *args)

    def contractDetailsEnd(self, request_id: int):
        super().contractDetailsEnd(request_id)
        self._handle_callback_end("contractDetails", request_id)

    def securityDefinitionOptionParameter(self, request_id: int, *args):
        super().securityDefinitionOptionParameter(request_id, *args)
        self._handle_callback("securityDefinitionOptionParameter", request_id, *args)

    def securityDefinitionOptionParameterEnd(self, request_id: int):
        """ Called when all callbacks to securityDefinitionOptionParameter are
        complete

        reqId - the ID used in the call to securityDefinitionOptionParameter """
        super().securityDefinitionOptionParameterEnd(request_id)
        self._handle_callback_end("securityDefinitionOptionParameter", request_id)

    def historicalData(self, request_id: int, *args):
        """ returns the requested historical data bars

        request_id - the request's identifier
        date  - the bar's date and time (either as a yyyymmdd hh:mm:ss formatted
             string or as system time according to the request)
        open  - the bar's open point
        high  - the bar's high point
        low   - the bar's low point
        close - the bar's closing point
        volume - the bar's traded volume if available
        barCount - the number of trades during the bar's timespan (only available
            for TRADES).
        average -   the bar's Weighted Average Price
        """
        super().historicalData(request_id, *args)
        self._handle_callback("historicalData", request_id, *args)

    def historicalDataEnd(self, request_id: int, *args):
        """ Marks the ending of the historical bars reception. """
        super().historicalDataEnd(request_id, *args)
        self._handle_callback_end("historicalData", request_id)
