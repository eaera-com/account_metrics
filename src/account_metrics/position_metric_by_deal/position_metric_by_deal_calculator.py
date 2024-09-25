from typing import Dict, Tuple, Annotated, Type
import pandas as pd

from account_metrics.metric_model import MetricData, MetricCalculator
from .position_metric_by_deal_data_model import PositionMetricByDeal
from account_metrics.metric_utils import apply_groupby_mapping_of_metric_to_data

from account_metrics.mt_deal_enum import EnDealEntry, EnDealAction

class PositionMetricByDealCalculator(MetricCalculator):
    input_class = ["Deal"]
    output_metric = PositionMetricByDeal
    groupby_field = [k for k, v in output_metric.model_fields.items() if "groupby" in v.metadata]

        
    @classmethod
    def validate_data(cls,input_data: Dict[str, pd.DataFrame]):
        for key in cls.input_class:
            if key not in input_data.keys():
                print(f"Key {key} not in input data = {input_data.keys()}")
                raise ValueError("Invalid input data")
        if(input_data['Deal'].empty):
            return False
        return True
    
    @classmethod
    def calculate(cls,input_data: Dict[str, pd.DataFrame], current_metric:Dict[Type[MetricData], Dict[tuple, pd.Series]]) -> pd.DataFrame:
        if not cls.validate_data(input_data):
            return pd.DataFrame(columns=cls.output_metric.model_fields.keys())
        
        map_position_id_to_deals: Dict[Tuple[Annotated[int, "server"], Annotated[int, "PositionID"]], pd.DataFrame] = apply_groupby_mapping_of_metric_to_data(cls.output_metric, input_data['Deal'])

        result: list[pd.Series] = []
        keys = list(map_position_id_to_deals.keys())

        for i in range(len(keys)):
            deals_of_login = map_position_id_to_deals[keys[i]]
            init_key_value = {k: keys[i][j] for j, k in enumerate(cls.groupby_field)}
            current_metric_of_login =current_metric.get(keys[i], pd.Series(cls.output_metric(**init_key_value).model_dump()))
            for index, deal in deals_of_login.iterrows():
                if deal["Deal"] <= current_metric_of_login["deal_id"]:
                    continue
                calculated_metric:PositionMetricByDeal = cls.calculate_row(deal, current_metric_of_login)
                new_rows = pd.Series(calculated_metric.model_dump())
                result.append(new_rows)
                current_metric_of_login = new_rows

        return pd.DataFrame(result, columns=cls.output_metric.model_fields.keys())
    
    @classmethod
    def calculate_row(cls, deal:pd.Series, prev:pd.Series) -> PositionMetricByDeal:
        metric = cls.output_metric()
        action = deal["Action"] if isinstance(deal["Action"], EnDealAction) else EnDealAction(deal["Action"])
        entry = deal["Entry"] if isinstance(deal["Entry"], EnDealEntry) else EnDealEntry(deal["Entry"])

        metric.server = deal["server"]
        metric.action = action.value if entry == EnDealEntry.ENTRY_IN else prev["action"]
        metric.comment = deal["Comment"] if entry == EnDealEntry.ENTRY_IN else prev["comment"]
        metric.commission = deal["Commission"] + prev["commission"]
        metric.deal_id = deal["Deal"]
        metric.digits = deal["Digits"] if entry == EnDealEntry.ENTRY_IN else prev["digits"]
        metric.digits_currency = deal["DigitsCurrency"]
        metric.login = deal["Login"]
        metric.position_id = deal["PositionID"]
        metric.price = deal["Price"]
        metric.price_position = deal["PricePosition"]
        metric.price_sl = deal["PriceSL"]
        metric.price_tp = deal["PriceTP"]
        metric.profit = deal["Profit"] + prev["profit"]
        metric.profit_raw = deal["ProfitRaw"] + prev["profit_raw"]
        metric.rate_margin = deal["RateMargin"]
        metric.storage = deal["Storage"] + prev["storage"]
        metric.symbol = deal["Symbol"]
        metric.timestamp_utc = deal["TimeUTC"]
        metric.timestamp_server = deal["Time"]
        metric.timestamp_open = deal["TimeUTC"] if entry == EnDealEntry.ENTRY_IN else prev["timestamp_open"]
        metric.timestamp_open_server = (
            deal["Time"] if entry == EnDealEntry.ENTRY_IN else prev["timestamp_open_server"]
        )
        metric.volume = deal["Volume"] if entry == EnDealEntry.ENTRY_IN else prev["volume"]
        metric.volume_closed = deal["VolumeClosed"] + prev["volume_closed"]
        metric.volume_remaining = metric.volume - metric.volume_closed
        metric.volume_ext = deal["VolumeExt"] if entry == EnDealEntry.ENTRY_IN else prev["volume_ext"]
        metric.volume_closed_ext = deal["VolumeClosedExt"] + prev["volume_closed_ext"]
        metric.volume_remaining_ext = metric.volume_ext - metric.volume_closed_ext
        metric.net_profit = metric.profit + metric.commission + metric.storage

        return metric
