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

RequestContractDetails = namedtuple("RequestContractDetails",
                                    ["ticker", "sec_type", "exchange", "currency"],
                                    defaults=["USD", "SMART", "STK"])
ResponseContractDetails = namedtuple("ResponseContractDetails", ["contract_details"])

RequestOptionDetails = namedtuple("RequestOptionDetails", ["contract"])
ResponseOptionDetails = namedtuple("ResponseOptionDetails", ["exchange", "stock_con_id",
                                                             "trading_class", "multiplier",
                                                             "expirations", "strikes"])

RequestHistorical = namedtuple("RequestHistorical",
                               ["contract", "endDateTime", "durationStr",
                                "barSizeSetting", "whatToShow", "useRTH", "formatDate",
                                "keepUpToDate", "chartOptions"],
                               defaults=[[], False, 1, 0, "TRADES", "30 min", "5 d"]
                               )
ResponseHistorical = namedtuple("ResponseHistorical", ["bar"])


class RequestWrapper(EWrapper):
    """ Wrapper that turns the callback-based IB API Wrapper into a blocking API, by collecting results into tables
    and returning the complete tables.
    """

    REQUEST_ID_STOCK_DETAILS = 1
    REQUEST_ID_SEC_DEF_OPT_PARAMS = 2
    REQUEST_OPTION_CHAIN = 3

    FIELDS_STOCK_DETAILS = [
        # from Contract
        "contract_id", "ticker", "exchange",
        # from ContractDetails
        "long_name", "industry", "category", "sub_category", "time_zone_id", "trading_hours", "liquid_hours"
    ]

    FIELDS_HISTORICAL = {
        "trades": ["open", "high", "low", "close", "volume", "barCount", "average"],
        "implied_vol": ["open", "high", "low", "close", "barCount", "average"],
        "historical_vol": ["open", "high", "low", "close", "barCount", "average"],
    }

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

    def request_option_params(self, contract: Contract):
        self.current_request_id = self.REQUEST_ID_SEC_DEF_OPT_PARAMS
        self._app.reqSecDefOptParams(self.current_request_id,
                                     contract.localSymbol,
                                     "",  # Leave blank so it will return all exchange options
                                     contract.secType,
                                     contract.conId)
        return self._get_response_table()

    def request_option_chain(self, ticker: str, exchange: str, expiration=datetime.date, currency="USD"):
        contract = Contract()
        contract.secType = "OPT"
        contract.symbol = ticker
        contract.exchange = exchange
        contract.currency = currency
        self.current_request_id = self.REQUEST_OPTION_CHAIN
        self._app.reqContractDetails(self.current_request_id, contract)
        return self._get_response_table()

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

        else:
            logger.error(f"Ignoring unexpected contractDetails call from request id {request_id}")
            return

    def contractDetailsEnd(self, request_id: int):
        super().contractDetailsEnd(request_id)
        if self.current_request_id == request_id:
            self.response_ready.set()
        else:
            logger.error(f"Ignoring unexpected contractDetailsEnd call from request id {request_id}")

    # def securityDefinitionOptionParameter(self, request_id: int, *args):
    #     super().securityDefinitionOptionParameter(request_id, *args)
    #     if not self._is_response_valid(request_id, RequestOptionDetails):
    #         logger.error(f"Ignoring unexpected contractDetails call from request id {request_id}")
    #         return
    #     self.response_table += [ResponseOptionDetails(*args)]
    #
    # def securityDefinitionOptionParameterEnd(self, request_id: int):
    #     """ Called when all callbacks to securityDefinitionOptionParameter are
    #     complete
    #
    #     reqId - the ID used in the call to securityDefinitionOptionParameter """
    #     super().securityDefinitionOptionParameterEnd(request_id)
    #     self._handle_task_finished(request_id)
    #
    # def historicalData(self, request_id: int, bar: BarData):
    #     """ returns the requested historical data bars
    #
    #     request_id - the request's identifier
    #     date  - the bar's date and time (either as a yyyymmdd hh:mm:ss formatted
    #          string or as system time according to the request)
    #     open  - the bar's open point
    #     high  - the bar's high point
    #     low   - the bar's low point
    #     close - the bar's closing point
    #     volume - the bar's traded volume if available
    #     barCount - the number of trades during the bar's timespan (only available
    #         for TRADES).
    #     average -   the bar's Weighted Average Price
    #     """
    #     super().historicalData(request_id, bar)
    #     if not self._is_response_valid(request_id, RequestHistorical):
    #         logger.error(f"Ignoring unexpected historicalData call from request id {request_id}")
    #         return
    #     self.response_table += [ResponseHistorical(bar=bar)]
    #
    # def historicalDataEnd(self, request_id:int, start: str, end: str):
    #     """ Marks the ending of the historical bars reception. """
    #     super().historicalDataEnd(request_id, start, end)
    #     self._handle_task_finished(request_id)
