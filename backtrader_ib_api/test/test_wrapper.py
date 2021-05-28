from backtrader_ib_api.wrapper.wrapper import RequestWrapper


def test_stock_details(wrapper: RequestWrapper):
    details = wrapper.request_stock_details("AAPL")
    print(details)


def test_stock_historical_trades(wrapper: RequestWrapper):
    history = wrapper.request_stock_trades_history("AAPL")
    print(history)


def test_option_lookup(wrapper: RequestWrapper):
    history = wrapper.request_stock_trades_history("AAPL")
    latest_price = history.close.iloc[-1]
    print(f"Latest price: {latest_price}")

    # contract ID can be requested via request_stock_details
    option_params = wrapper.request_option_params("AAPL", 265598)
    print(f"Found {len(option_params)} option param results")

    # try to auto-select SMART exchange
    preferred_contracts = option_params[option_params.exchange == "SMART"]
    option_contract = preferred_contracts.iloc[0]
    front_expiration = min(option_contract.expirations)
    print(f"Front Expiration: {front_expiration}")

    option_chain = wrapper.request_option_chain("AAPL", option_contract.exchange, front_expiration)
    front_strike = option_chain.strike[option_chain.strike > latest_price].iloc[0]
    print(f"Closest OTM Call Strike: {front_strike}")

    option_price_history = wrapper.request_option_trades_history("AAPL",
                                                                 front_expiration,
                                                                 front_strike,
                                                                 "C")
    print(option_price_history)
