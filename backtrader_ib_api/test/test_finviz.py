import datetime
from backtrader_ib_api.finviz import get_stock_info, estimate_next_earnings_date


def test_stock_info():
    stock_info = get_stock_info("AAPL")
    print(stock_info)


def test_estimate_next_earnings_date():
    next_earnings_date = estimate_next_earnings_date("AAPL")
    print(next_earnings_date)
    assert next_earnings_date
    assert next_earnings_date > datetime.datetime.today()
