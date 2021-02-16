from backtrader_ib_api.request_wrapper import RequestWrapper, RequestContractDetails


def test_stock_details(wrapper: RequestWrapper):
    details = wrapper.request_stock_details("AAPL")
    print(details)


def test_option_params(wrapper: RequestWrapper):
    # contract ID can be requested via request_stock_details
    option_params = wrapper.request_option_params("AAPL", 265598)
    print(option_params)
