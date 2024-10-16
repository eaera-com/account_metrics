from typing import Dict, Type
import pandas as pd

from account_metrics.metric_model import MetricData, MetricCalculator


# NOT YET USED. NEED TO BE INCOPORATED INTO DATA MODEL AND OTHER QUERY FROM CLICKHOUSE
# TODO: INCOPORATED INTO DATA MODEL AND OTHER QUERY FROM CLICKHOUSE (use same format for all data models)
# TODO: INCORPORATE WITH TYPE TRANSLATOR FROM MT5Manager
class MT5DealCalculator(MetricCalculator):
    def calculate(cls, input_data: pd.DataFrame) -> pd.DataFrame:
        result = input_data.copy(deep=True)
        result = result.drop('Login', axis=1)

        result.rename(columns={
            "Deal": "deal",
            "ExternalID": "external_id",
            "Dealer": "dealer",
            "Order": "order",
            "Action": "action",
            "Entry": "entry",
            "Digits": "digits",
            "DigitsCurrency": "digits_currency",
            "ContractSize": "contract_size",
            "Time": "time",
            "Symbol": "symbol",
            "Price": "price",
            "Volume": "volume",
            "Profit": "profit",
            "Storage": "storage",
            "Commission": "commission",
            "RateProfit": "rate_profit",
            "RateMargin": "rate_margin",
            "ExpertID": "expert_id",
            "PositionID": "position_id",
            "Comment": "comment",
            "ProfitRaw": "profit_raw",
            "PricePosition": "price_position",
            "VolumeClosed": "volume_closed",
            "TickValue": "tick_value",
            "TickSize": "tick_size",
            "Flags": "flags",
            "TimeMsc": "time_msc",
            "Reason": "reason",
            "Gateway": "gateway",
            "PriceGateway": "price_gateway",
            "ModificationFlags": "modification_flags",
            "PriceSL": "price_sl",
            "PriceTP": "price_tp",
            "VolumeExt": "volume_ext",
            "VolumeClosedExt": "volume_closed_ext",
            "Fee": "fee",
            "Value": "value",
            "MarketBid": "market_bid",
            "MarketAsk": "market_ask",
            "MarketLast": "market_last",
            "server": "server",
            "login": "login",
            "TimeUTC": "time_utc",
            "deal_id": "deal_id",
            "timestamp_server": "timestamp_server"
        },inplace=True)
        return result