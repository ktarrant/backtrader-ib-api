import logging
import pystore
from typing import Set
import datetime
import pandas as pd
from collections import namedtuple

from ibapi.common import BarData, TickerId
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import ContractDetails, Contract

from .stocks import ContractArgs, SP100_HOLDINGS

logger = logging.getLogger(__name__)

REQUEST_SEQUENCE = ["contract_details", "chain", "details", "historical"]
REQUEST_ID_TO_NAME = dict(enumerate(REQUEST_SEQUENCE))
REQUEST_NAME_TO_ID = {value: key for key, value in REQUEST_ID_TO_NAME.items()}
# REQUEST_VALUE_TO_NAME = {value: key for key, value in REQUEST_NAME_TO_VALUE.items()}
BAR_FIELDS = {
    "trades": ["open", "high", "low", "close", "volume", "barCount", "average"],
    "implied_vol": ["open", "high", "low", "close", "barCount", "average"],
    "historical_vol": ["open", "high", "low", "close", "barCount", "average"],
}

RequestSecDefDetails = namedtuple("RequestSecDefDetails",
                                  ["reqId", "underlyingSymbol", "futFopExchange",
                                   "underlyingSecType", "underlyingConId"])


class OptionChainWrapper(EWrapper):
    """ Wrapper that is used to find the correct expiration and then download historical data for
    all strikes in that expiration
    """

    def __init__(self, store, contract: ContractArgs = None, expiration_after: datetime.datetime = None,
                 duration="5 d", bar_size="30 mins", after_hours=False):
        EWrapper.__init__(self)
        self._app = None

        if not contract:
            contract = SP100_HOLDINGS[0].contract
        self.contract = contract

        if not bar_size:
            raise ValueError("Bar size must be provided, for example '30 mins'")
        bar_size_str = bar_size.replace("mins", "m").replace(" ", "")

        self.collection = store.collection(f"options-{bar_size_str}")

        if not expiration_after:
            expiration_after = datetime.datetime.today()
        self.expiration_after = expiration_after

        end_time = datetime.datetime.today()
        query_time = end_time.strftime("%Y%m%d %H:%M:%S")
        self.current_request_id = -1
        # response objects
        self.table = None
        self.expirations = set()
        self.contract_details = None

    @property
    def app(self):
        if self._app is None:
            raise ValueError("app has not been set. Assign app property before starting run loop.")
        return self._app

    @app.setter
    def app(self, value):
        self._app = value

    def send_next_request(self):
        """ Kicks off the next historical data request
        """
        self.current_request_id += 1
        request = REQUEST_SEQUENCE[self.current_request_id]
        if request == "contract_details":
            self._app.reqContractDetails(self.current_request_id, self.contract)

        elif request == "chain":
            self._app.reqSecDefOptParams(self.current_request_id,
                                         self.contract.localSymbol,
                                         "",  # hopefully SMART picks the best options market
                                         self.contract.secType,
                                         self.contract.conId)

        elif request == "details":
            option_contract = Contract()
            option_contract.secType = "OPT"
            option_contract.exchange = "SMART"
            option_contract.currency = "USD"
            option_contract.symbol = self.contract.localSymbol
            front_expiration = min(self.expirations)
            print(f"Using expiration: {front_expiration}")
            option_contract.lastTradeDateOrContractMonth = front_expiration
            self._app.reqContractDetails(self.current_request_id, option_contract)

        else:
            # if all the requests are done, shut down the app
            # self.app.disconnect()
            print("All requests are complete. You can shut down the app now.")

    def error(self, req_id: TickerId, error_code: int, error_string: str):
        """This event is called when there is an error with the
        communication or when TWS wants to send a message to the client."""
        logger.error(f"{error_string} (req_id:{req_id}, error_code:{error_code})")

        if (2000 <= error_code < 10000) or (error_code == 10167):
            logger.error(f"Error code {error_code} is non-fatal, ignoring it.")
        else:
            logger.fatal(f"Error code {error_code} is fatal, disconnecting now.")
            # self._app.disconnect()

    def connectAck(self):
        super().connectAck()
        logger.info("Connection successful. Sending first request...")
        self.send_next_request()

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        print("Contract details: " + str(contractDetails))
        if REQUEST_SEQUENCE[reqId] == "contract_details":
            self.contract_details = contractDetails
            # update contract info with complete data
            self.contract = self.contract_details.contract

    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        print("ContractDetailsEnd. ReqId:", reqId)
        self.send_next_request()

    def securityDefinitionOptionParameter(self, reqId: int, exchange: str, underlyingConId: int,
                                          tradingClass: str, multiplier: str, expirations: Set[str],
                                          strikes: Set[float]):
        super().securityDefinitionOptionParameter(reqId, exchange, underlyingConId, tradingClass, multiplier,
                                                  expirations, strikes)
        if exchange == "SMART":
            print(f"{self.contract.localSymbol} expirations: {expirations}")
            print(f"{self.contract.localSymbol} strikes: {strikes}")
            # TODO: Pick the correct expiration from the list
            self.expirations = expirations

    def securityDefinitionOptionParameterEnd(self, reqId:int):
        """ Called when all callbacks to securityDefinitionOptionParameter are
        complete

        reqId - the ID used in the call to securityDefinitionOptionParameter """
        super().securityDefinitionOptionParameterEnd(reqId)
        self.send_next_request()

    # def historicalData(self, reqId: int, bar: BarData):
    #     """ returns the requested historical data bars
    #
    #     reqId - the request's identifier
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
    #
    #     logger.info(f"Received bar data: {bar}")
    #     dt = datetime.datetime.strptime(bar.date, "%Y%m%d %H:%M:%S")
    #     request = self.requests[reqId]
    #     request_name = REQUEST_VALUE_TO_NAME[request.whatToShow]
    #     self.table.loc[dt] = [getattr(bar, field) for field in BAR_FIELDS[request_name]]
    #
    # def historicalDataEnd(self, reqId:int, start:str, end:str):
    #     """ Marks the ending of the historical bars reception. """
    #     super().historicalDataEnd(reqId, start, end)
    #     request = self.requests[reqId]
    #     request_name = REQUEST_VALUE_TO_NAME[request.whatToShow]
    #     contract_name = request.contract.localSymbol
    #     logger.info(f"'{request_name}' data finished for ticker '{contract_name}', writing to collection")
    #     # if table already exists, pull it in and we will update
    #     # otherwise create a fresh table
    #     try:
    #         # pystore should automatically drop any duplicates, updating the data with latest if there are any
    #         self.collections[request_name].append(contract_name, self.table)
    #     except ValueError:
    #         self.collections[request_name].write(contract_name, self.table)
    #     self.current_request_id += 1
    #     if len(self.requests) == self.current_request_id:
    #         # if all the requests are done, shut down the app
    #         # self.app.disconnect()
    #         print("All requests are complete. You can shut down the app now.")
    #     else:
    #         self.send_next_request()


if __name__ == "__main__":
    import argparse
    import datetime

    parser = argparse.ArgumentParser(description="""
    Collecting equity price historical data
    """)

    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=7496, type=int)
    parser.add_argument("--clientid", default=0, type=int)
    parser.add_argument("--storage-path", default='C:/stores', help="Path to store downloaded data")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    # Set storage path
    pystore.set_path(args.storage_path)
    store = pystore.store("ib")

    wrapper = OptionChainWrapper(store)
    app = EClient(wrapper=wrapper)
    wrapper.app = app
    app.connect(args.host, args.port, args.clientid)
    app.run()
