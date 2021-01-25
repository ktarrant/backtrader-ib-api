import logging
import pystore
from typing import List
import pandas as pd
from collections import namedtuple

from ibapi.common import BarData, TickerId
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

from .stocks import ContractArgs, SP100_HOLDINGS

logger = logging.getLogger(__name__)

DEFAULT_REQUESTS = ["trades", "implied_vol", "historical_vol"]
REQUEST_NAME_TO_ID = {
    "trades": 1,
    "implied_vol": 2,
    "historical_vol": 3,
}
REQUEST_ID_TO_NAME = {value: key for key, value in REQUEST_NAME_TO_ID.items()}
REQUEST_NAME_TO_VALUE = {
    "trades": "TRADES",
    "implied_vol": "OPTION_IMPLIED_VOLATILITY",
    "historical_vol": "HISTORICAL_VOLATILITY",
}
REQUEST_VALUE_TO_NAME = {value: key for key, value in REQUEST_NAME_TO_VALUE.items()}
BAR_FIELDS = {
    "trades": ["open", "high", "low", "close", "volume", "barCount", "average"],
    "implied_vol": ["open", "high", "low", "close", "barCount", "average"],
    "historical_vol": ["open", "high", "low", "close", "barCount", "average"],
}

RequestHistorical = namedtuple("RequestHistorical",
                               ["reqId", "contract", "endDateTime", "durationStr",
                                "barSizeSetting", "whatToShow", "useRTH", "formatDate",
                                "keepUpToDate", "chartOptions"])


class EquityPriceDataWrapper(EWrapper):
    """ Wrapper that is used to collect historical price, implied volatility,
    and historical volatility data for a list of contracts
    """

    @staticmethod
    def get_contract_for_ticker(ticker: str, sec_type="STK", exchange="SMART", currency="USD"):
        """ Returns a Contract object suitable for using to request data for
        ticker (uses localSymbol field for the ticker)
        """
        contract = Contract()
        contract.secType = sec_type
        contract.exchange = exchange
        contract.currency = currency
        contract.localSymbol = ticker
        return contract

    def __init__(self, store, contracts: List[ContractArgs] = None, requests: List[str] = None,
                 duration="6 M", bar_size="30 mins", after_hours=False):
        EWrapper.__init__(self)
        self._app = None

        if not bar_size:
            raise ValueError("Bar size must be provided, for example '30 mins'")
        bar_size_str = bar_size.replace("mins", "m").replace(" ", "")

        if not requests:
            requests = DEFAULT_REQUESTS
        self.collections = {request: store.collection(f"{request}-{bar_size_str}") for request in requests}

        if not contracts:
            contracts = SP100_HOLDINGS
        contracts = [self.get_contract_for_ticker(contract.ticker, exchange=contract.exchange)
                     for contract in contracts]

        end_time = datetime.datetime.today()
        query_time = end_time.strftime("%Y%m%d %H:%M:%S")
        self.table = None
        self.requests = [
            RequestHistorical(
                reqId=i,
                contract=params[1],
                endDateTime=query_time,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=REQUEST_NAME_TO_VALUE[params[0]],
                useRTH=0 if after_hours else 1,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[],
            )
            for i, params in enumerate([(request_name, contract)
                                        for contract in contracts
                                        for request_name in requests])
        ]
        self.current_request_id = 0

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
        request = self.requests[self.current_request_id]
        request_name = REQUEST_VALUE_TO_NAME[request.whatToShow]
        self.table = pd.DataFrame([], columns=BAR_FIELDS[request_name])
        self._app.reqHistoricalData(*request)

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
        logger.info("Connection successful. Requesting historical data...")
        self.send_next_request()

    def historicalData(self, reqId: int, bar: BarData):
        """ returns the requested historical data bars

        reqId - the request's identifier
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

        logger.info(f"Received bar data: {bar}")
        dt = datetime.datetime.strptime(bar.date, "%Y%m%d %H:%M:%S")
        request = self.requests[reqId]
        request_name = REQUEST_VALUE_TO_NAME[request.whatToShow]
        self.table.loc[dt] = [getattr(bar, field) for field in BAR_FIELDS[request_name]]

    def historicalDataEnd(self, reqId:int, start:str, end:str):
        """ Marks the ending of the historical bars reception. """
        super().historicalDataEnd(reqId, start, end)
        request = self.requests[reqId]
        request_name = REQUEST_VALUE_TO_NAME[request.whatToShow]
        contract_name = request.contract.localSymbol
        logger.info(f"'{request_name}' data finished for ticker '{contract_name}', writing to collection")
        # if table already exists, pull it in and we will update
        # otherwise create a fresh table
        try:
            # pystore should automatically drop any duplicates, updating the data with latest if there are any
            self.collections[request_name].append(contract_name, self.table)
        except ValueError:
            self.collections[request_name].write(contract_name, self.table)
        self.current_request_id += 1
        if len(self.requests) == self.current_request_id:
            # if all the requests are done, shut down the app
            # self.app.disconnect()
            print("All requests are complete. You can shut down the app now.")
        else:
            self.send_next_request()


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

    wrapper = EquityPriceDataWrapper(store)
    app = EClient(wrapper=wrapper)
    wrapper.app = app
    app.connect(args.host, args.port, args.clientid)
    app.run()
