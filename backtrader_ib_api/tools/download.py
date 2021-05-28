import logging
import pystore

from backtrader_ib_api.wrapper.wrapper import RequestWrapper

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import argparse
    import datetime

    from backtrader_ib_api.finviz import estimate_next_earnings_date
    from backtrader_ib_api.tools.stocks import SP100_HOLDINGS, FAVES_HOLDINGS

    parser = argparse.ArgumentParser(description="""
    Downloads historical trades and options bid/ask data for a list of tickers using the IB Trader Workstation API
    """)

    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=7496, type=int)
    parser.add_argument("--clientid", default=0, type=int)
    parser.add_argument("--storage-path", default='C:/stores', help="Path to store downloaded data")
    parser.add_argument("--tickers", default="aapl", help="Comma-separated, case-insensitive list of tickers")
    parser.add_argument("--short", action="store_true", help="Add 'Short List' holdings to the tickers list")
    parser.add_argument("--faves", action="store_true", help="Add 'Faves' holdings to the tickers list")
    parser.add_argument("--sp100", action="store_true", help="Add S&P 100 holdings to the tickers list")
    parser.add_argument("--exchange", default="SMART", help="Exchange for the ticker")
    parser.add_argument("--bar-size", default="30 mins", help="Bar size")
    parser.add_argument("--duration", default="5d", help="Duration value to look back. Valid units are d, w, m, y")
    parser.add_argument("--expiration", default=None, type=str,
                        help="Expiration in YYYYMMDD format. If none is provided, "
                             "the system computes front expiration after next earnings")

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
    duration = f"{args.duration[:-1]} {args.duration[-1]}"

    today = datetime.datetime.today()
    if args.expiration:
        expiration_dt = datetime.datetime.strptime(args.expiration, "%Y%m%d")

        if expiration_dt < today:
            query_time_str = expiration_dt.strftime("%Y%m%d %H:%M:%S")
        else:
            query_time_str = today.strftime("%Y%m%d %H:%M:%S")
    else:
        query_time_str = today.strftime("%Y%m%d %H:%M:%S")

    equity_trades_collection = store.collection(f"trades-{bar_size_str}")
    equity_iv_collection = store.collection(f"impliedvol-{bar_size_str}")
    equity_hv_collection = store.collection(f"historicalvol-{bar_size_str}")
    option_bidask_collection = store.collection(f"option-bidask-{bar_size_str}")

    tickers = set(ticker.upper() for ticker in args.tickers.split(","))
    if args.sp100:
        tickers = tickers.union(set(contract.ticker.upper() for contract in SP100_HOLDINGS))
    if args.faves:
        tickers = tickers.union(set(contract.ticker.upper() for contract in FAVES_HOLDINGS))

    for ticker in tickers:
        stock_details = wrapper.request_stock_details(ticker)
        logger.info(f"Using first match found for ticker {ticker}:\n{stock_details}")
        stock_contract_id = stock_details.index[0]
        ticker_metadata = dict(stock_details.loc[stock_contract_id])
        ticker_metadata["contract_id"] = int(stock_contract_id)

        history = wrapper.request_stock_trades_history(ticker, duration=duration, bar_size=args.bar_size,
                                                       query_time=query_time_str)
        logger.debug(f"Equity history request for {ticker} finished")
        try:
            # pystore should automatically drop any duplicates, updating the data with latest if there are any
            equity_trades_collection.append(ticker, history)
        except ValueError:
            equity_trades_collection.write(ticker, history, metadata=ticker_metadata)

        iv_history = wrapper.request_stock_iv_history(ticker, duration=duration, bar_size=args.bar_size,
                                                      query_time=query_time_str)
        logger.debug(f"IV history request for {ticker} finished")
        try:
            # pystore should automatically drop any duplicates, updating the data with latest if there are any
            equity_iv_collection.append(ticker, history)
        except ValueError:
            equity_iv_collection.write(ticker, history, metadata=ticker_metadata)

        hv_history = wrapper.request_stock_iv_history(ticker, duration=duration, bar_size=args.bar_size,
                                                      query_time=query_time_str)
        logger.debug(f"HV history request for {ticker} finished")
        try:
            # pystore should automatically drop any duplicates, updating the data with latest if there are any
            equity_hv_collection.append(ticker, history)
        except ValueError:
            equity_hv_collection.write(ticker, history, metadata=ticker_metadata)

        option_params = wrapper.request_option_params(ticker, stock_contract_id)
        logger.info(f"Found {len(option_params)} option param results")

        # try to auto-select SMART exchange
        preferred_contracts = option_params[option_params.exchange == args.exchange]
        option_contract = preferred_contracts.iloc[0]
        expiration_dates = [datetime.datetime.strptime(expiration, "%Y%m%d")
                            for expiration in option_contract.expirations]

        if args.expiration is None:
            # find expiration just after earnings
            earnings_date = estimate_next_earnings_date(ticker)
            earnings_expiration_date = min(expiration for expiration in expiration_dates if expiration > earnings_date)
            logger.info(f"Closest Options Expiration for Earnings: {earnings_expiration_date}")
            earnings_expiration = datetime.datetime.strftime(earnings_expiration_date, "%Y%m%d")
        else:
            if args.expiration not in option_contract.expirations:
                logger.error(f"Invalid expiration provided: {args.expiration}. "
                             f"Valid expirations: {option_contract.expirations}")
                continue
            earnings_expiration = args.expiration
            logger.info(f"Using provided Expiration: {earnings_expiration}")

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
                                                            bar_size=args.bar_size,
                                                            query_time=query_time_str)
            item_name = f"{ticker}-{earnings_expiration}-{strike}{right}"
            logger.debug(f"{item_name} history: {history}")
            try:
                # pystore should automatically drop any duplicates, updating the data with latest if there are any
                option_bidask_collection.append(item_name, history)
            except ValueError:
                option_metadata = dict(option_chain.loc[contract_id])
                option_metadata["stock_ticker"] = ticker
                option_metadata["stock_contract_id"] = int(stock_contract_id)
                option_metadata["option_contract_id"] = int(contract_id)
                option_bidask_collection.write(item_name, history, metadata=option_metadata)

    wrapper.stop_app()
