from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup
import datetime
import logging

logger = logging.getLogger(__name__)


def get_stock_info(ticker: str):
    url = ("http://finviz.com/quote.ashx?t=" + ticker.lower())
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urlopen(req) as webobj:
        soup = BeautifulSoup(webobj.read(), "html.parser")
        table_columns = [td.text for td in soup.find_all(attrs={"class": "snapshot-td2-cp"})]
        table_values = [td.text for td in soup.find_all(attrs={"class": "snapshot-td2"})]
        return pd.Series(table_values, index=table_columns)


def estimate_next_earnings_date(ticker: str):
    stock_info = get_stock_info(ticker)
    try:
        month, day, session = stock_info.loc["Earnings"].split(" ")
    except ValueError:
        # earnings information is malformed or not provided
        return None

    today = datetime.datetime.today()
    listed = datetime.datetime(year=today.year,
                               month=datetime.datetime.strptime(month, "%b").month,
                               day=int(day) + 1 if session == "AMC" else int(day))
    candidates = [listed,
                  listed + datetime.timedelta(weeks=52),
                  listed + datetime.timedelta(weeks=13)]
    winner = min(dt for dt in candidates if dt > today)
    return winner
