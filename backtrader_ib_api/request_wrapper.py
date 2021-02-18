from collections import namedtuple
import logging
from datetime import datetime
from threading import Event, Thread
import pandas as pd

from ibapi.common import BarData, TickerId
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import ContractDetails, Contract

logger = logging.getLogger(__name__)


class RequestWrapper(EWrapper):
    """ Wrapper that turns the callback-based IB API Wrapper into a blocking API, by collecting results into tables
    and returning the complete tables.
    """

    REQUEST_ID_STOCK_DETAILS = 1
    REQUEST_ID_SEC_DEF_OPT_PARAMS = 2
    REQUEST_ID_OPTION_DETAILS = 3
    REQUEST_ID_STOCK_TRADES_HISTORY = 4
    # REQUEST_ID_HISTORICAL_IV = 5
    # REQUEST_ID_HISTORICAL_HV = 6

    FIELDS_STOCK_DETAILS = [
        # from Contract
        "contract_id", "ticker", "exchange",
        # from ContractDetails
        "long_name", "industry", "category", "sub_category", "time_zone_id", "trading_hours", "liquid_hours"
    ]

    FIELDS_OPTION_PARAMS = [
        "exchange", "multiplier", "expirations", "strikes",
    ]

    FIELDS_OPTION_DETAILS = [
        # from Contract
        "contract_id", "option_ticker", "exchange", "expiration", "strike", "right", "multiplier",
    ]

    FIELDS_HISTORICAL_TRADES = ["open", "high", "low", "close", "volume", "count", "average"]
    # FIELDS_HISTORICAL_VOL = ["open", "high", "low", "close", "count", "average"]

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

    def _get_response_table(self):
        self.response_ready.wait(timeout=self.timeout)
        self.response_ready.clear()
        return self.response_table

    def request_stock_details(self, ticker: str, exchange="SMART", currency="USD"):
        contract = Contract()
        contract.secType = "STK"
        contract.localSymbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        self.current_request_id = self.REQUEST_ID_STOCK_DETAILS
        self.response_table = pd.DataFrame(columns=self.FIELDS_STOCK_DETAILS)
        self._app.reqContractDetails(self.current_request_id, contract)
        return self._get_response_table()

    def request_option_params(self, ticker: str, contract_id: int):
        self.current_request_id = self.REQUEST_ID_SEC_DEF_OPT_PARAMS
        self.response_table = pd.DataFrame(columns=self.FIELDS_OPTION_PARAMS)
        self._app.reqSecDefOptParams(self.current_request_id,
                                     ticker,
                                     "",  # Leave blank so it will return all exchange options
                                     "STK",
                                     contract_id)
        return self._get_response_table()

    def request_option_chain(self, ticker: str, exchange: str, expiration: str, currency="USD"):
        contract = Contract()
        contract.secType = "OPT"
        contract.symbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        contract.lastTradeDateOrContractMonth = expiration
        self.current_request_id = self.REQUEST_ID_OPTION_DETAILS
        self.response_table = pd.DataFrame(columns=self.FIELDS_OPTION_DETAILS)
        self._app.reqContractDetails(self.current_request_id, contract)
        return self._get_response_table()

    def _request_historical(self, request_id: int, contract: Contract, columns: list,
                            data_type="TRADES", duration="5 d", bar_size="30 mins", after_hours=False):
        end_time = datetime.today()
        query_time = end_time.strftime("%Y%m%d %H:%M:%S")
        self.current_request_id = request_id
        self.response_table = pd.DataFrame(columns=columns)
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
        contract = Contract()
        contract.secType = "STK"
        contract.localSymbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        return self._request_historical(self.REQUEST_ID_STOCK_TRADES_HISTORY,
                                        contract,
                                        self.FIELDS_HISTORICAL_TRADES,
                                        **kwargs)

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

    def contractDetails(self, request_id: int, contract_details: ContractDetails):
        super().contractDetails(request_id, contract_details)
        if request_id != self.current_request_id:
            logger.error(f"Ignoring unexpected contractDetails call from request id {request_id}")
            return

        if request_id == self.REQUEST_ID_STOCK_DETAILS:
            self.response_table.loc[len(self.response_table.index)] = [
                contract_details.contract.conId,
                contract_details.contract.symbol,
                contract_details.contract.exchange,
                contract_details.longName,
                contract_details.industry,
                contract_details.category,
                contract_details.subcategory,
                contract_details.timeZoneId,
                contract_details.tradingHours,
                contract_details.liquidHours,
            ]

        elif request_id == self.REQUEST_ID_OPTION_DETAILS:
            self.response_table.loc[len(self.response_table.index)] = [
                contract_details.contract.conId,
                contract_details.contract.localSymbol,
                contract_details.contract.exchange,
                contract_details.contract.lastTradeDateOrContractMonth,
                contract_details.contract.strike,
                contract_details.contract.right,
                contract_details.contract.multiplier,
            ]
        else:
            logger.error(f"Ignoring unexpected contractDetails call from request id {request_id}")
            return

    def contractDetailsEnd(self, request_id: int):
        super().contractDetailsEnd(request_id)
        if self.current_request_id == request_id:
            self.response_ready.set()
        else:
            logger.error(f"Ignoring unexpected contractDetailsEnd call from request id {request_id}")

    def securityDefinitionOptionParameter(self, request_id: int, exchange:str,
                                          underlyingConId:int, tradingClass:str, multiplier:str,
                                          expirations, strikes):
        super().securityDefinitionOptionParameter(request_id, exchange, underlyingConId, tradingClass,
                                                  multiplier, expirations, strikes)
        if request_id != self.current_request_id:
            logger.error(f"Ignoring unexpected securityDefinitionOptionParameter call from request id {request_id}")
            return

        if request_id == self.REQUEST_ID_SEC_DEF_OPT_PARAMS:
            self.response_table.loc[len(self.response_table.index)] = [
                exchange,
                multiplier,
                expirations,
                strikes,
            ]

        else:
            logger.error(f"Ignoring unexpected securityDefinitionOptionParameter call from request id {request_id}")
            return

    def securityDefinitionOptionParameterEnd(self, request_id: int):
        """ Called when all callbacks to securityDefinitionOptionParameter are
        complete

        reqId - the ID used in the call to securityDefinitionOptionParameter """
        super().securityDefinitionOptionParameterEnd(request_id)
        if self.current_request_id == request_id and request_id == self.REQUEST_ID_SEC_DEF_OPT_PARAMS:
            self.response_ready.set()
        else:
            logger.error(f"Ignoring unexpected securityDefinitionOptionParameterEnd call from request id {request_id}")

    def historicalData(self, request_id: int, bar: BarData):
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
        super().historicalData(request_id, bar)
        if request_id != self.current_request_id:
            logger.error(f"Ignoring unexpected historicalData call from request id {request_id}")
            return

        if request_id == self.REQUEST_ID_STOCK_TRADES_HISTORY:
            dt = datetime.strptime(bar.date, "%Y%m%d %H:%M:%S")
            self.response_table.loc[dt] = [
                bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average
            ]

        # if request_id == self.REQUEST_ID_HISTORICAL_IV or request_id == self.REQUEST_ID_HISTORICAL_HV:
        #     self.response_table.loc[len(self.response_table.index)] = [
        #         bar.open, bar.high, bar.low, bar.close, bar.barCount, bar.average
        #     ]

        else:
            logger.error(f"Ignoring unexpected securityDefinitionOptionParameter call from request id {request_id}")
            return

    def historicalDataEnd(self, request_id:int, start: str, end: str):
        """ Marks the ending of the historical bars reception. """
        super().historicalDataEnd(request_id, start, end)
        if self.current_request_id == request_id and request_id == self.REQUEST_ID_STOCK_TRADES_HISTORY:
            self.response_ready.set()
        else:
            logger.error(f"Ignoring unexpected securityDefinitionOptionParameterEnd call from request id {request_id}")
