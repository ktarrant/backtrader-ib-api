import datetime
from ibapi.contract import Contract


class FuturesBasket:
    EXPIRATION_LABELS = {
        1: "F",
        2: "G",
        3: "H",
        4: "J",
        5: "K",
        6: "M",
        7: "N",
        8: "Q",
        9: "U",
        10: "V",
        11: "X",
        12: "Z",
    }

    @staticmethod
    def futures_contract(ticker: str, exchange: str):
        # ! [futcontract_local_symbol]
        contract = Contract()
        contract.secType = "FUT"
        contract.exchange = exchange
        contract.currency = "USD"
        contract.localSymbol = ticker
        # ! [futcontract_local_symbol]
        return contract

    @staticmethod
    def local_symbol(base: str, expiration_date: datetime.datetime):
        expiration_label = FuturesBasket.EXPIRATION_LABELS[
            expiration_date.month]
        year_suffix = str(expiration_date.year)[-1:]
        ticker = f"{base}{expiration_label}{year_suffix}"
        return ticker

    @property
    def symbols(self):
        return ["ES", "NQ", "RTY"]
        # return ["GC", "HG", "SI"]

    @property
    def exchange(self):
        return "GLOBEX"
        # return "NYMEX"

    @property
    def roll_offset(self):
        return 8
        # return 7

    def get_expiration_date(self, year, month):
        """ third Friday in the month """
        fridays = [d for d in range(1, 22) if
                   datetime.datetime(year=year, month=month,
                                     day=d).weekday() == 4]
        return datetime.datetime(year=year, month=month, day=fridays[2])

    # def get_expiration_date(self, year, month):
    #     _, last_day = monthrange(year, month)
    #     bizdays = [d for d in range(last_day, 21, -1)
    #                if (1 <= datetime.datetime(year=year,
    #                                           month=month,
    #                                           day=d).weekday()
    #                    <= 5)]
    #     return datetime.datetime(year=year, month=month, day=bizdays[-3])

    def get_expiration_months(self, symbol: str):
        return [3, 6, 9, 12]

    # def get_expiration_months(self, symbol: str):
    #     if symbol is "GC":
    #         return [2, 4, 6, 8, 10, 12]
    #     elif symbol in ["HG", "SI"]:
    #         return [3, 5, 7, 9, 12]
    #     else:
    #         raise NotImplementedError()

    def get_expiration_dates(self, symbol: str, year: int):
        expiration_months = self.get_expiration_months(symbol)
        expiration_months = expiration_months + [expiration_months[0]]
        expiration_years = [year] * 4 + [year + 1]
        return [self.get_expiration_date(y, m)
                for y, m in zip(expiration_years, expiration_months)]

    def generate_requests(self):
        today = datetime.datetime.today()
        for base in self.symbols:
            expiration_dates = self.get_expiration_dates(base, today.year)
            expiration_date = next(expiration_date
                                   for expiration_date in expiration_dates
                                   if expiration_date >= today)
            roll_date = (expiration_date
                         - datetime.timedelta(days=self.roll_offset))
            ticker = FuturesBasket.local_symbol(base, expiration_date)
            contract = FuturesBasket.futures_contract(ticker, self.exchange)
            end_date = min(roll_date, today)
            yield contract, end_date