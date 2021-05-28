import logging
from datetime import datetime
from threading import Thread, Event

from ibapi.common import TickerId
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

from .responses import (Response, StockDetailsResponse, OptionDetailsResponse,
                        OptionParamsResponse, HistoricalTradesResponse,
                        HistoricalDataResponse, HistoricalBidAskResponse)

logger = logging.getLogger(__name__)


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

    def __init__(self, timeout: int = None):
        """
        Create an EWrapper to provide blocking access to the callback-based IB API.
        :param timeout: Amount of time in seconds to wait for a response before giving up. Use None to never give up.
        """
        EWrapper.__init__(self)
        self.timeout = timeout
        self._app = None
        self.connected = Event()
        self.pending_responses = {}
        self.next_request_id = 0
        self.thread = None

    def start_app(self, host: str, port: int, client_id: int):
        """ Start a connection ton IB TWS application in a background thread and confirm connection is successful.
        :param host: Hostname to connect to, usually 127.0.0.1
        :param port: Port to connect to, configurable and differs for live vs paper trading.
        :param client_id: Client ID setting for the TWS API
        """
        self._app = EClient(wrapper=self)
        self.connected.clear()
        self.next_request_id = 0
        self._app.connect(host, port, client_id)
        self.thread = Thread(target=self._app.run, daemon=True)
        self.thread.start()
        # connectAck will set the connected event once called
        self.connected.wait(timeout=self.timeout)

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
        response = StockDetailsResponse()
        request_id = self._start_request(response)
        contract = self._get_stock_contract(ticker, **kwargs)
        self._app.reqContractDetails(request_id, contract)
        response.finished.wait(timeout=self.timeout)
        return response.table

    def request_option_params(self, ticker: str, contract_id: int):
        """ Request options expiration and strike information about the provided stock ticker and contract_id.
        :param ticker: stock ticker with available options
        :param contract_id: contract ID of the stock with available options, returned by request_stock_details
        """
        response = OptionParamsResponse()
        request_id = self._start_request(response)
        self._app.reqSecDefOptParams(request_id,
                                     ticker,
                                     "",  # Leave blank so it will return all exchange options
                                     "STK",
                                     contract_id)
        response.finished.wait(timeout=self.timeout)
        return response.table

    def request_option_chain(self, ticker: str, exchange: str, expiration: str, currency="USD"):
        """ Request a list of all the options available for a given ticker and expiration.
        :param ticker: stock ticker with available options
        :param exchange: exchange of the options contracts
        :param expiration: expiration of the options contracts, in YYYYMMDD format
        :param currency: currency to report information in
        """
        response = OptionDetailsResponse()
        request_id = self._start_request(response)
        # do not use _get_option_contract shortcut because we are leaving right and strike blank
        contract = Contract()
        contract.secType = "OPT"
        contract.symbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiration
        self._app.reqContractDetails(request_id, contract)
        response.finished.wait(timeout=self.timeout)
        return response.table

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
        response = HistoricalTradesResponse()
        request_id = self._start_request(response)
        contract = self._get_stock_contract(ticker, **kwargs)
        self._request_historical(request_id, contract, "TRADES", **kwargs)
        response.finished.wait(timeout=self.timeout)
        return response.table

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
        response = HistoricalDataResponse()
        request_id = self._start_request(response)
        contract = self._get_stock_contract(ticker, **kwargs)
        self._request_historical(request_id, contract, "OPTION_IMPLIED_VOLATILITY", **kwargs)
        response.finished.wait(timeout=self.timeout)
        return response.table

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
        response = HistoricalDataResponse()
        request_id = self._start_request(response)
        contract = self._get_stock_contract(ticker, **kwargs)
        self._request_historical(request_id, contract, "HISTORICAL_VOLATILITY", **kwargs)
        response.finished.wait(timeout=self.timeout)
        return response.table

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
        response = HistoricalTradesResponse()
        request_id = self._start_request(response)
        contract = self._get_option_contract(ticker, expiration, strike, right, **kwargs)
        self._request_historical(request_id, contract, "TRADES", **kwargs)
        response.finished.wait(timeout=self.timeout)
        return response.table

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
        response = HistoricalBidAskResponse()
        request_id = self._start_request(response)
        contract = self._get_option_contract(ticker, expiration, strike, right, **kwargs)
        self._request_historical(request_id, contract, "BID_ASK", **kwargs)
        response.finished.wait(timeout=self.timeout)
        return response.table

    def _start_request(self, response: Response) -> int:
        """ Gets a request id for a new request, associates it with the given response object,
        then returns the new request id.
        """
        current_id = self.next_request_id
        self.next_request_id += 1
        self.pending_responses[current_id] = response
        return current_id

    @staticmethod
    def _get_stock_contract(ticker: str, exchange="SMART", currency="USD", **_):
        """ Helper function for creating a contract object for use in querying
        data for stocks
        """
        contract = Contract()
        contract.secType = "STK"
        contract.localSymbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        return contract

    @staticmethod
    def _get_option_contract(ticker: str, expiration: str, strike: float, right: str,
                             exchange="SMART", currency="USD", **_):
        """ Helper function for creating a contract object for use in querying
        data for options
        """
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

    def _request_historical(self, request_id: int, contract: Contract, data_type: str, duration="5 d",
                            bar_size="30 mins", query_time=datetime.today().strftime("%Y%m%d %H:%M:%S"),
                            after_hours=False, **_):
        """ Helper function used to send a request for historical data
        """
        if data_type not in self.REQUEST_OPTIONS_HISTORICAL_TYPE:
            raise ValueError(f"Invalid data type '{data_type}'. Valid options: {self.REQUEST_OPTIONS_HISTORICAL_TYPE}")

        if bar_size not in self.REQUEST_OPTIONS_BAR_SIZE:
            raise ValueError(f"Invalid data type '{bar_size}'. Valid options: {self.REQUEST_OPTIONS_BAR_SIZE}")

        self._app.reqHistoricalData(reqId=request_id,
                                    contract=contract,
                                    endDateTime=query_time,
                                    durationStr=duration,
                                    barSizeSetting=bar_size,
                                    whatToShow=data_type,
                                    useRTH=0 if after_hours else 1,
                                    formatDate=1,
                                    keepUpToDate=False,
                                    chartOptions=[])

    def _handle_callback(self, callback_name, request_id, *args):
        """ Helper function for IB API callbacks to call to notify the pending
        response object of new data
        """
        try:
            response = self.pending_responses[request_id]
        except KeyError:
            logger.error(f"Unexpected callback {callback_name} had invalid"
                         f"request id '{request_id}'")
            return

        response.handle_response(callback_name, *args)

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
            self._handle_callback("error", req_id, error_code, error_string)

    def connectAck(self):
        super().connectAck()
        logger.info("Connection successful.")
        self.connected.set()

    def contractDetails(self, request_id: int, *args):
        super().contractDetails(request_id, *args)
        self._handle_callback("contractDetails", request_id, *args)

    def contractDetailsEnd(self, request_id: int):
        super().contractDetailsEnd(request_id)
        self._handle_callback("contractDetailsEnd", request_id)

    def securityDefinitionOptionParameter(self, request_id: int, *args):
        super().securityDefinitionOptionParameter(request_id, *args)
        self._handle_callback("securityDefinitionOptionParameter", request_id, *args)

    def securityDefinitionOptionParameterEnd(self, request_id: int):
        """ Called when all callbacks to securityDefinitionOptionParameter are
        complete

        reqId - the ID used in the call to securityDefinitionOptionParameter """
        super().securityDefinitionOptionParameterEnd(request_id)
        self._handle_callback("securityDefinitionOptionParameterEnd", request_id)

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
        self._handle_callback("historicalDataEnd", request_id)
