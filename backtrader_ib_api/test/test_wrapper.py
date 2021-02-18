from backtrader_ib_api.request_wrapper import RequestWrapper


def test_stock_details(wrapper: RequestWrapper):
    details = wrapper.request_stock_details("AAPL")
    print(details)


def test_option_chain(wrapper: RequestWrapper):
    # contract ID can be requested via request_stock_details
    option_params = wrapper.request_option_params("AAPL", 265598)
    print(f"Found {len(option_params)} option param results")

    # try to auto-select SMART exchange
    preferred_contracts = option_params[option_params.exchange == "SMART"]
    option_contract = preferred_contracts.iloc[0]
    front_expiration = min(option_contract.expirations)
    print(f"Front Expiration: {front_expiration}")

    option_chain = wrapper.request_option_chain("AAPL", option_contract.exchange, front_expiration)
    print(option_chain)


def test_stock_historical_trades(wrapper: RequestWrapper):
    history = wrapper.request_stock_trades_history("AAPL")
    print(history)
