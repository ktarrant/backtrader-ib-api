import logging
import datetime
import pystore
from typing import List, Dict
import pandas as pd

from ibapi.common import BarData, TickerId
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

logger = logging.getLogger(__name__)

DEFAULT_REQUESTS = ["trades"]
REQUEST_NAME_TO_ID = {
    "trades": 1,
    "implied_vol": 2,
    "historical_vol": 3,
}
REQUEST_ID_TO_NAME = {value: key for key, value in REQUEST_NAME_TO_ID.items()}
REQUEST_VALUES = {
    "trades": "TRADES",
    "implied_vol": "OPTION_IMPLIED_VOLATILITY",
    "historical_vol": "HISTORICAL_VOLATILITY",
}


class SnapshotWrapper(EWrapper):
    BAR_FIELDS = ["open", "high", "low", "close", "volume", "barCount", "average"]

    def __init__(self, app, contract_name: str, collections: dict):
        EWrapper.__init__(self)
        self.app = app
        self.contract_name = contract_name
        self.collections = collections
        self.tables = {request: pd.DataFrame([], columns=self.BAR_FIELDS)
                       for request in self.collections}
        self.requests_finished = []

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
        self.app.send_req_historical()

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
        request_name = REQUEST_ID_TO_NAME[reqId]
        table = self.tables[request_name]
        table.loc[dt] = [getattr(bar, field) for field in self.BAR_FIELDS]

    def historicalDataEnd(self, reqId:int, start:str, end:str):
        """ Marks the ending of the historical bars reception. """
        super().historicalDataEnd(reqId, start, end)
        req_name = REQUEST_ID_TO_NAME[reqId]
        logger.info(f"Bar data finished: {req_name}, writing to collection")
        # if table already exists, pull it in and we will update
        # otherwise create a fresh table
        try:
            # pystore should automatically drop any duplicates, updating the data with latest if there are any
            self.collections[req_name].append(self.contract_name, self.tables[req_name])
        except ValueError:
            self.collections[req_name].write(self.contract_name, self.tables[req_name])
        self.requests_finished.append(req_name)
        if all([request in self.requests_finished for request in self.collections]):
            # if all the requests are done, shut down the app
            # self.app.disconnect()
            print("All requests are complete. You can shut down the app now.")


class SnapshotApp(EClient):
    def __init__(self, contract: Contract, store: pystore.store, requests: List[str] = None,
                 duration="6 M", bar_size="30 mins", after_hours=False):

        if not bar_size:
            raise ValueError("Bar size must be provided, for example '30 mins'")
        bar_size_str = bar_size.replace("mins", "m").replace(" ", "")

        end_time = datetime.datetime.today()
        query_time = end_time.strftime("%Y%m%d %H:%M:%S")

        if not requests:
            requests = DEFAULT_REQUESTS
        self.collections = {request: store.collection(f"{request}-{bar_size_str}") for request in requests}

        if not contract.localSymbol:
            raise ValueError("Contract.localSymbol must be provided, for example 'SPY'")
        wrapper = SnapshotWrapper(self, contract.localSymbol, self.collections)
        EClient.__init__(self, wrapper=wrapper)
        self.requests = [
            (
                REQUEST_NAME_TO_ID[request_name],
                contract,
                query_time,
                duration,
                bar_size,
                REQUEST_VALUES[request_name],
                0 if after_hours else 1,
                1,
                False,  # keep up to date
                []
            )
            for request_name in requests
        ]

    def send_req_historical(self):
        for request in self.requests:
            self.reqHistoricalData(*request)


if __name__ == "__main__":
    import argparse
    import datetime

    parser = argparse.ArgumentParser(description="""
    Collecting futures historical data
    """)

    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=7496, type=int)
    parser.add_argument("--clientid", default=0, type=int)
    parser.add_argument("--exchange", default="SMART", help="Exchange to use for contract lookup, default is SMART")
    parser.add_argument("--sec-type", default="STK", help="Security type to look up, default is STK (stock)")
    parser.add_argument("--currency", default="USD", help="Currency to use for quotes, default is USD (US Dollar)")
    parser.add_argument("--storage-path", default='C:/stores', help="Path to store downloaded data")
    parser.add_argument("ticker", help="Ticker to look up")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    # Set storage path
    pystore.set_path(args.storage_path)
    store = pystore.store("ib")

    contract = Contract()
    contract.secType = args.sec_type
    contract.exchange = args.exchange
    contract.currency = args.currency
    contract.localSymbol = args.ticker

    app = SnapshotApp(contract, store)
    app.connect(args.host, args.port, args.clientid)
    app.run()
