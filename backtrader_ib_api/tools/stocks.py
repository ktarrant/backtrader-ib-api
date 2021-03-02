from ibapi.contract import Contract


class ContractArgs:
    """ Helper class for creating a contract with a one-line initializer
    """
    def __init__(self, ticker: str, name: str, exchange: str = "SMART", currency: str = "USD"):
        self._contract = Contract()
        self._contract.secType = "STK"
        self._contract.exchange = exchange
        self._contract.currency = currency
        self._contract.localSymbol = ticker
        self._name = name

    @property
    def contract(self):
        return self._contract

    @property
    def ticker(self):
        return self._contract.localSymbol

    @property
    def name(self):
        return self._name


SP100_HOLDINGS = [
    ContractArgs(ticker="AAPL", name="Apple Inc."),
    ContractArgs(ticker="ABBV", name="AbbVie Inc."),
    ContractArgs(ticker="ABT", name="Abbott Laboratories"),
    ContractArgs(ticker="ACN", name="Accenture"),
    ContractArgs(ticker="ADBE", name="Adobe Inc."),
    ContractArgs(ticker="AIG", name="American International Group"),
    ContractArgs(ticker="ALL", name="Allstate"),
    ContractArgs(ticker="AMGN", name="Amgen Inc."),
    ContractArgs(ticker="AMT", name="American Tower"),
    ContractArgs(ticker="AMZN", name="Amazon.com"),
    ContractArgs(ticker="AXP", name="American Express"),
    ContractArgs(ticker="BA", name="Boeing Co."),
    ContractArgs(ticker="BAC", name="Bank of America Corp"),
    ContractArgs(ticker="BIIB", name="Biogen"),
    ContractArgs(ticker="BK", name="The Bank of New York Mellon"),
    ContractArgs(ticker="BKNG", name="Booking Holdings"),
    ContractArgs(ticker="BLK", name="BlackRock Inc"),
    ContractArgs(ticker="BMY", name="Bristol-Myers Squibb"),
    ContractArgs(ticker="BRK B", name="Berkshire Hathaway"),
    ContractArgs(ticker="C", name="Citigroup Inc"),
    ContractArgs(ticker="CAT", name="Caterpillar Inc."),
    ContractArgs(ticker="CHTR", name="Charter Communications"),
    ContractArgs(ticker="CL", name="Colgate-Palmolive"),
    ContractArgs(ticker="CMCSA", name="Comcast Corp."),
    ContractArgs(ticker="COF", name="Capital One Financial Corp."),
    ContractArgs(ticker="COP", name="ConocoPhillips"),
    ContractArgs(ticker="COST", name="Costco Wholesale Corp."),
    ContractArgs(ticker="CRM", name="salesforce.com"),
    ContractArgs(ticker="CSCO", name="Cisco Systems"),
    ContractArgs(ticker="CVS", name="CVS Health"),
    ContractArgs(ticker="CVX", name="Chevron Corporation"),
    ContractArgs(ticker="DD", name="DuPont de Nemours Inc"),
    ContractArgs(ticker="DHR", name="Danaher Corporation"),
    ContractArgs(ticker="DIS", name="The Walt Disney Company"),
    ContractArgs(ticker="DOW", name="Dow Inc."),
    ContractArgs(ticker="DUK", name="Duke Energy"),
    ContractArgs(ticker="EMR", name="Emerson Electric Co."),
    ContractArgs(ticker="EXC", name="Exelon"),
    ContractArgs(ticker="F", name="Ford Motor Company"),
    ContractArgs(ticker="FB", name="Facebook, Inc."),
    ContractArgs(ticker="FDX", name="FedEx"),
    ContractArgs(ticker="GD", name="General Dynamics"),
    ContractArgs(ticker="GE", name="General Electric"),
    ContractArgs(ticker="GILD", name="Gilead Sciences"),
    ContractArgs(ticker="GM", name="General Motors"),
    ContractArgs(ticker="GOOG", name="Alphabet Inc. (Class C)"),
    ContractArgs(ticker="GOOGL", name="Alphabet Inc. (Class A)"),
    ContractArgs(ticker="GS", name="Goldman Sachs"),
    ContractArgs(ticker="HD", name="The Home Depot"),
    ContractArgs(ticker="HON", name="Honeywell"),
    ContractArgs(ticker="IBM", name="International Business Machines"),
    ContractArgs(ticker="INTC", name="Intel Corp."),
    ContractArgs(ticker="JNJ", name="Johnson & Johnson"),
    ContractArgs(ticker="JPM", name="JPMorgan Chase & Co."),
    ContractArgs(ticker="KHC", name="Kraft Heinz"),
    ContractArgs(ticker="KMI", name="Kinder Morgan"),
    ContractArgs(ticker="KO", name="The Coca-Cola Company"),
    ContractArgs(ticker="LLY", name="Eli Lilly and Company"),
    ContractArgs(ticker="LMT", name="Lockheed Martin"),
    ContractArgs(ticker="LOW", name="Lowe's"),
    ContractArgs(ticker="MA", name="MasterCard Inc"),
    ContractArgs(ticker="MCD", name="McDonald's Corp"),
    ContractArgs(ticker="MDLZ", name="Mondelēz International"),
    ContractArgs(ticker="MDT", name="Medtronic plc"),
    ContractArgs(ticker="MET", name="MetLife Inc."),
    ContractArgs(ticker="MMM", name="3M Company"),
    ContractArgs(ticker="MO", name="Altria Group"),
    ContractArgs(ticker="MRK", name="Merck & Co."),
    ContractArgs(ticker="MS", name="Morgan Stanley"),
    ContractArgs(ticker="MSFT", name="Microsoft"),
    ContractArgs(ticker="NEE", name="NextEra Energy"),
    ContractArgs(ticker="NFLX", name="Netflix"),
    ContractArgs(ticker="NKE", name="Nike, Inc."),
    ContractArgs(ticker="NVDA", name="Nvidia Corporation"),
    ContractArgs(ticker="ORCL", name="Oracle Corporation"),
    ContractArgs(ticker="PEP", name="PepsiCo"),
    ContractArgs(ticker="PFE", name="Pfizer Inc"),
    ContractArgs(ticker="PG", name="Procter & Gamble"),
    ContractArgs(ticker="PM", name="Philip Morris International"),
    ContractArgs(ticker="PYPL", name="PayPal"),
    ContractArgs(ticker="QCOM", name="Qualcomm"),
    ContractArgs(ticker="RTX", name="Raytheon Technologies"),
    ContractArgs(ticker="SBUX", name="Starbucks Corp."),
    ContractArgs(ticker="SLB", name="Schlumberger"),
    ContractArgs(ticker="SO", name="Southern Company"),
    ContractArgs(ticker="SPG", name="Simon Property Group"),
    ContractArgs(ticker="T", name="AT&T Inc"),
    ContractArgs(ticker="TGT", name="Target Corporation"),
    ContractArgs(ticker="TMO", name="Thermo Fisher Scientific"),
    ContractArgs(ticker="TSLA", name="Tesla, Inc."),
    ContractArgs(ticker="TXN", name="Texas Instruments"),
    ContractArgs(ticker="UNH", name="UnitedHealth Group"),
    ContractArgs(ticker="UNP", name="Union Pacific Corporation"),
    ContractArgs(ticker="UPS", name="United Parcel Service"),
    ContractArgs(ticker="USB", name="U.S. Bancorp"),
    ContractArgs(ticker="V", name="Visa Inc."),
    ContractArgs(ticker="VZ", name="Verizon Communications"),
    ContractArgs(ticker="WBA", name="Walgreens Boots Alliance"),
    ContractArgs(ticker="WFC", name="Wells Fargo"),
    ContractArgs(ticker="WMT", name="Walmart"),
    ContractArgs(ticker="XOM", name="Exxon Mobil Corp"),
]
