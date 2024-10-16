from typing import Dict, Tuple, Annotated, Type, Any
import pandas as pd


from .position_metric_by_deal_data_model import PositionMetricByDeal

from account_metrics.metric_model import MetricData, MetricCalculator
from account_metrics.mt_deal_enum import EnDealEntry, EnDealAction
from account_metrics.mt5_deal import MT5Deal
from account_metrics.datastore import Datastore
from account_metrics.basic_deal_calculator import BasicDealMetricCalculator


class PositionMetricByDealCalculator(BasicDealMetricCalculator):
    input_class = MT5Deal
    addtional_data = [PositionMetricByDeal]
    output_metric = PositionMetricByDeal
    groupby_field = [k for k, v in output_metric.model_fields.items() if "groupby" in v.metadata]
    
    def __init__(self,datastore:Datastore):
        super().__init__(datastore)
    
    def calculate_row(self, deal:pd.Series, prev:pd.Series, additional_df:Dict[MetricData,pd.DataFrame]) -> PositionMetricByDeal:
        metric = self.__class__.output_metric()
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
