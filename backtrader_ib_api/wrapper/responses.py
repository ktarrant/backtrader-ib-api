from typing import List
import pandas as pd
from datetime import datetime
import threading
import logging

from ibapi.common import BarData
from ibapi.contract import ContractDetails

logger = logging.getLogger(__name__)


class Response:
    """ Generic response class. """
    row_callback = None
    end_callback = None
    columns: List[str] = []

    def __init__(self):
        self.finished = threading.Event()
        self._response_table = pd.DataFrame(columns=self.columns)

    def parse_data(self, *args) -> List:
        """ Converts the arguments passed in the callback from IB into data
        fields to be added as a row to the data table.
        """
        return list(args)

    def handle_response(self, callback, *args):
        """ Handle a callback for IB API. If the callback was handled and
        there is more callbacks expected, returns True. If the callback is
        unexpected or there is no more callbacks expected, returns False.
        """
        if callback == self.row_callback:
            data = self.parse_data(*args)
            self._response_table.loc[len(self._response_table.index)] = data

        elif callback == self.end_callback:
            self.finished.set()

        else:
            logger.error(f"Unexpected callback '{callback}'")
            self.finished.set()

    @property
    def table(self):
        """ Response data table, where each row is a response.
        """
        return self._response_table


class StockDetailsResponse(Response):
    """ Response class for request contract details about a stock """
    row_callback = "contractDetails"
    end_callback = "contractDetailsEnd"
    columns = [
        # from Contract
        "ticker", "exchange",
        # from ContractDetails
        "long_name", "industry", "category", "sub_category", "time_zone_id", "trading_hours", "liquid_hours"
    ]

    def parse_data(self, contract_details: ContractDetails, *args) -> List:
        return [
            contract_details.contract.symbol,
            contract_details.contract.exchange,
            contract_details.longName,
            contract_details.industry,
            contract_details.category,
            contract_details.subcategory,
            contract_details.timeZoneId,
            contract_details.tradingHours,
            contract_details.liquidHours,
        ]


class OptionParamsResponse(Response):
    """ Response class for request option parameters """
    row_callback = "securityDefinitionOptionParameter"
    end_callback = "securityDefinitionOptionParameterEnd"
    columns = ["exchange", "multiplier", "expirations", "strikes"]

    def parse_data(self, exchange, _, __, multiplier, expirations, strikes, *args) -> List:
        return [
            exchange,
            multiplier,
            expirations,
            strikes,
        ]


class OptionDetailsResponse(Response):
    """ Response class for request contract details about a stock """
    row_callback = "contractDetails"
    end_callback = "contractDetailsEnd"
    columns = [
        # from Contract
        "option_ticker", "exchange", "expiration", "strike", "right", "multiplier",
    ]

    def parse_data(self, contract_details: ContractDetails, *args) -> List:
        return [
            contract_details.contract.localSymbol,
            contract_details.contract.exchange,
            contract_details.contract.lastTradeDateOrContractMonth,
            contract_details.contract.strike,
            contract_details.contract.right,
            contract_details.contract.multiplier,
        ]


class HistoricalTradesResponse(Response):
    """ Response class for historical data requests """
    row_callback = "historicalData"
    end_callback = "historicalDataEnd"
    columns = ["datetime", "open", "high", "low", "close", "volume", "count", "average"]
    renames = {"barCount": "count"}

    def parse_data(self, bar: BarData, *args) -> List:
        dt = datetime.strptime(bar.date, "%Y%m%d %H:%M:%S")
        values = [getattr(bar, self.renames.get(column, column))
                  for column in self.columns]
        return [dt] + values


class HistoricalDataResponse(Response):
    """ Response class for historical trades requests """
    row_callback = "historicalData"
    end_callback = "historicalDataEnd"
    columns = ["open", "high", "low", "close", "count", "average"]


class HistoricalBidAskResponse(Response):
    """ Response class for historical trades requests """
    row_callback = "historicalData"
    end_callback = "historicalDataEnd"
    columns = ["average_bid", "max_ask", "min_bid", "average_ask"]
    renames = {
        "open": "average_bid",
        "high": "max_ask",
        "low": "min_bid",
        "close": "average_ask"
    }
