import logging
from datetime import datetime
from threading import Event, Thread
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

    REQUEST_IDS = dict(
        STOCK_DETAILS=1,
        OPTION_PARAMS=2,
        OPTION_DETAILS=3,
        HISTORICAL_TRADES_EQUITY=4,
        HISTORICAL_TRADES_OPTIONS=5,
        HISTORICAL_BID_ASK_OPTIONS=6,
        HISTORICAL_IV_EQUITY=7,
        HISTORICAL_HV_EQUITY=8,
    )
    REQUEST_ID_TO_NAME = {v: k for k, v in REQUEST_IDS.items()}

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
        HISTORICAL_BID_ASK_OPTIONS=["average_bid", "max_ask", "min_bid", "average_ask", "count", "average"],
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
        HISTORICAL_BID_ASK_OPTIONS=historical_data_row_function,
        HISTORICAL_IV_EQUITY=historical_data_row_function,
        HISTORICAL_HV_EQUITY=historical_data_row_function,
    )

    def __init__(self, timeout=10.0):
        EWrapper.__init__(self)
        self.timeout = timeout
        self._app = None
        self.current_request_id = 0
        self.response_table = None
        self.response_ready = Event()
        self.thread = None

    def start_app(self, host, port, client_id):
        self._app = EClient(wrapper=self)
        self.response_table = None
        self.response_ready.clear()
        self._app.connect(host, port, client_id)
        self.thread = Thread(target=self._app.run, daemon=True)
        self.thread.start()
        self.response_ready.wait(timeout=self.timeout)
        self.response_ready.clear()

    def stop_app(self):
        self._app.disconnect()
        self.thread.join()

    @property
    def app(self):
        return self._app

    def _start_request(self, request_name):
        self.current_request_id = self.REQUEST_IDS[request_name]
        self.response_table = pd.DataFrame(columns=self.REQUEST_FIELDS[request_name])

    def _get_response_table(self):
        self.response_ready.wait(timeout=self.timeout)
        self.response_ready.clear()
        return self.response_table

    def request_stock_details(self, ticker: str, exchange="SMART", currency="USD"):
        self._start_request("STOCK_DETAILS")
        contract = Contract()
        contract.secType = "STK"
        contract.localSymbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        self._app.reqContractDetails(self.current_request_id, contract)
        return self._get_response_table()

    def request_option_params(self, ticker: str, contract_id: int):
        self._start_request("OPTION_PARAMS")
        self._app.reqSecDefOptParams(self.current_request_id,
                                     ticker,
                                     "",  # Leave blank so it will return all exchange options
                                     "STK",
                                     contract_id)
        return self._get_response_table()

    def request_option_chain(self, ticker: str, exchange: str, expiration: str, currency="USD"):
        self._start_request("OPTION_DETAILS")
        contract = Contract()
        contract.secType = "OPT"
        contract.symbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiration
        self._app.reqContractDetails(self.current_request_id, contract)
        return self._get_response_table()

    def _request_historical(self, contract: Contract,
                            data_type="TRADES", duration="5 d", bar_size="30 mins", after_hours=False):
        if data_type not in self.REQUEST_OPTIONS_HISTORICAL_TYPE:
            raise ValueError(f"Invalid data type '{data_type}'. Valid options: {self.REQUEST_OPTIONS_HISTORICAL_TYPE}")

        if bar_size not in self.REQUEST_OPTIONS_BAR_SIZE:
            raise ValueError(f"Invalid data type '{bar_size}'. Valid options: {self.REQUEST_OPTIONS_BAR_SIZE}")

        end_time = datetime.today()
        query_time = end_time.strftime("%Y%m%d %H:%M:%S")
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
        return self._get_response_table()

    def request_stock_trades_history(self, ticker: str, exchange="SMART", currency="USD", **kwargs):
        self._start_request("HISTORICAL_TRADES_EQUITY")
        contract = Contract()
        contract.secType = "STK"
        contract.localSymbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        return self._request_historical(contract, **kwargs)

    def request_option_trades_history(self, ticker: str, expiration: str, strike: float, right="C",
                                      exchange="SMART", currency="USD", **kwargs):
        self._start_request("HISTORICAL_TRADES_OPTIONS")
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
        return self._request_historical(contract, **kwargs)

    def error(self, req_id: TickerId, error_code: int, error_string: str):
        """This event is called when there is an error with the
        communication or when TWS wants to send a message to the client."""
        logger.error(f"{error_string} (req_id:{req_id}, error_code:{error_code})")

        # if 2000 <= error_code < 10000:
        #     return False
        # elif error_code == 10167: # delayed market data instead
        #     return False
        # else:
        #     return True

    def connectAck(self):
        logger.info("Connection successful.")
        self.response_ready.set()

    def _handle_callback(self, callback_name, request_id, *args):
        if request_id != self.current_request_id:
            logger.error(f"Ignoring unexpected {callback_name} call from request id {request_id} "
                         f"while processing other request id {self.current_request_id}")
            return

        try:
            request_name = self.REQUEST_ID_TO_NAME[request_id]
        except KeyError:
            logger.error(f"Ignoring unexpected {callback_name} from invalid request_id: {request_id}")
            return

        expected_callback_name = self.REQUEST_CALLBACKS[request_name]
        if expected_callback_name != callback_name:
            logger.error(f"Ignoring unexpected callback {callback_name}, expected callback {expected_callback_name}")
            return

        index, row = self.RESPONSE_ROW_FUNCTION[request_name](*args)
        if index is None:
            index = len(self.response_table.index)
        self.response_table.loc[index] = row

    def _handle_callback_end(self, callback_name, request_id):
        if request_id != self.current_request_id:
            logger.error(f"Ignoring unexpected {callback_name}End call from request id {request_id} "
                         f"while processing other request id {self.current_request_id}")
            return

        try:
            request_name = self.REQUEST_ID_TO_NAME[request_id]
        except KeyError:
            logger.error(f"Ignoring unexpected {callback_name}End from invalid request_id: {request_id}")
            return

        expected_callback_name = self.REQUEST_CALLBACKS[request_name]
        if expected_callback_name != callback_name:
            logger.error(f"Ignoring unexpected callback {callback_name}End, "
                         f"expected callback {expected_callback_name}End")
            return

        # notify that no more responses expected
        self.response_ready.set()

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
