import logging
import pystore
import pandas as pd

from backtrader_ib_api.request_wrapper import RequestWrapper

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


if __name__ == "__main__":
    import argparse
    import datetime

    parser = argparse.ArgumentParser(description="""
    Downloads data using the IB Trader Workstation API
    """)

    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=7496, type=int)
    parser.add_argument("--clientid", default=0, type=int)
    parser.add_argument("--storage-path", default='C:/stores', help="Path to store downloaded data")
    parser.add_argument("--tickers", default="aapl", help="Comma-separated, case-insensitive list of tickers")
    parser.add_argument("--bar-size", default="30 mins", help="Bar size")
    parser.add_argument("--equity-history", default=5, help="Number of days to collect")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    # Set storage path
    pystore.set_path(args.storage_path)
    store = pystore.store("ib")

    wrapper = RequestWrapper()
    wrapper.start_app(args.host, args.port, args.clientid)

    bar_size_str = args.bar_size.replace("mins", "m").replace(" ", "")
    equity_history_collection = store.collection(f"trades-{bar_size_str}")

    tickers = [ticker.upper() for ticker in args.tickers.split(",")]
    for ticker in tickers:
        stock_details = wrapper.request_stock_details(ticker)
        logger.info(f"Found Match for ticker {ticker}:\n{stock_details}")

        if args.equity_history:
            duration = f"{args.equity_history} d"
            history = wrapper.request_stock_trades_history(ticker, duration=duration, bar_size=args.bar_size)
            logger.info(f"Equity history: {history}")
            try:
                # pystore should automatically drop any duplicates, updating the data with latest if there are any
                equity_history_collection.append(ticker, history)
            except ValueError:
                equity_history_collection.write(ticker, history)

    wrapper.stop_app()
