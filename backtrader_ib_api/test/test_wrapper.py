from backtrader_ib_api.request_wrapper import RequestWrapper, RequestContractDetails


def test_stock_details(wrapper: RequestWrapper):
    details = wrapper.request_stock_details("AAPL")
    print(details)
