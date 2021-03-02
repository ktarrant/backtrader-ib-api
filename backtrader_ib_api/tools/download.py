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

    from backtrader_ib_api.finviz import estimate_next_earnings_date
    from backtrader_ib_api.tools.stocks import SP100_HOLDINGS

    parser = argparse.ArgumentParser(description="""
    Downloads data using the IB Trader Workstation API
    """)

    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=7496, type=int)
    parser.add_argument("--clientid", default=0, type=int)
    parser.add_argument("--storage-path", default='C:/stores', help="Path to store downloaded data")
    parser.add_argument("--tickers", default="aapl", help="Comma-separated, case-insensitive list of tickers")
    parser.add_argument("--sp100", action="store_true", help="Add S&P 100 holdings to the tickers list")
    parser.add_argument("--exchange", default="SMART", help="Exchange for the ticker")
    parser.add_argument("--bar-size", default="30 mins", help="Bar size")
    parser.add_argument("--history-length", default=5, help="Number of days to collect history data")

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
    duration = f"{args.history_length} d"

    equity_trades_collection = store.collection(f"trades-{bar_size_str}")
    # option_trades_collection = store.collection(f"option-trades-{bar_size_str}")
    option_bidask_collection = store.collection(f"option-bidask-{bar_size_str}")

    tickers = set(ticker.upper() for ticker in args.tickers.split(","))
    tickers = tickers.union(set(contract.ticker.upper() for contract in SP100_HOLDINGS))

    for ticker in tickers:
        stock_details = wrapper.request_stock_details(ticker)
        logger.info(f"Using first match found for ticker {ticker}:\n{stock_details}")
        stock_contract_id = stock_details.index[0]

        history = wrapper.request_stock_trades_history(ticker, duration=duration, bar_size=args.bar_size)
        logger.debug(f"Equity history: {history}")
        try:
            # pystore should automatically drop any duplicates, updating the data with latest if there are any
            equity_trades_collection.append(ticker, history)
        except ValueError:
            equity_trades_collection.write(ticker, history)

        option_params = wrapper.request_option_params(ticker, stock_contract_id)
        logger.info(f"Found {len(option_params)} option param results")

        # try to auto-select SMART exchange
        preferred_contracts = option_params[option_params.exchange == args.exchange]
        option_contract = preferred_contracts.iloc[0]

        # find expiration just after earnings
        earnings_date = estimate_next_earnings_date(ticker)
        expiration_dates = [datetime.datetime.strptime(expiration, "%Y%m%d")
                            for expiration in option_contract.expirations]
        earnings_expiration_date = min(expiration for expiration in expiration_dates if expiration > earnings_date)
        logger.info(f"Closest Options Expiration for Earnings: {earnings_expiration_date}")

        earnings_expiration = datetime.datetime.strftime(earnings_expiration_date, "%Y%m%d")
        option_chain = wrapper.request_option_chain(ticker, option_contract.exchange, earnings_expiration)
        logger.info(f"Option chain for {ticker} {earnings_expiration} has {len(option_chain.index)} options")
        logger.debug(f"Option chain: {option_chain}")

        for contract_id in option_chain.index:
            strike = option_chain.loc[contract_id, "strike"]
            right = option_chain.loc[contract_id, "right"]
            history = wrapper.request_option_bidask_history(ticker,
                                                            earnings_expiration,
                                                            strike,
                                                            right,
                                                            duration=duration,
                                                            bar_size=args.bar_size)
            item_name = f"{ticker}-{earnings_expiration}-{strike}{right}"
            logger.debug(f"{item_name} history: {history}")
            try:
                # pystore should automatically drop any duplicates, updating the data with latest if there are any
                option_bidask_collection.append(item_name, history)
            except ValueError:
                option_bidask_collection.write(item_name, history)

    wrapper.stop_app()
