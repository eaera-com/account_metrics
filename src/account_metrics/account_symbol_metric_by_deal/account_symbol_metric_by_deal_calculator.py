from typing import Dict, Tuple, Annotated, Type, Any
import pandas as pd


from .account_symbol_metric_by_deal_data_model import AccountSymbolMetricByDeal

from account_metrics.metric_model import MetricData
from account_metrics.mt_deal_enum import EnDealAction, EnDealEntry
from account_metrics.mt5_deal.mt5_deal_data_model import MT5Deal
from account_metrics.datastore import Datastore
from account_metrics.basic_deal_calculator import BasicDealMetricCalculator


class AccountSymbolMetricByDealCalculator(BasicDealMetricCalculator):
        
    input_class = MT5Deal
    addtional_data = [AccountSymbolMetricByDeal]
    output_metric = AccountSymbolMetricByDeal
    groupby_field = [k for k, v in output_metric.model_fields.items() if "groupby" in v.metadata]

    def __init__(self,datastore:Datastore):
        super().__init__(datastore)
        
    def calculate_row(self,deal:pd.Series, prev:pd.Series, additional_df:Dict[MetricData,pd.DataFrame]) -> AccountSymbolMetricByDeal:
        metric = self.__class__.output_metric()
        action = deal["Action"] if isinstance(deal["Action"], EnDealAction) else EnDealAction(deal["Action"])
        entry = deal["Entry"] if isinstance(deal["Entry"], EnDealEntry) else EnDealEntry(deal["Entry"])

        metric.server = deal["server"]
        metric.login = deal["Login"]
        metric.deal_id = deal["Deal"]
        metric.deal_profit = (
            deal["Profit"]
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else 0.0
        )
        metric.timestamp_server = deal["Time"]
        metric.timestamp_utc = deal["TimeUTC"]
        metric.symbol = deal["Symbol"]
        metric.total_profit = prev["total_profit"] + (
            deal["Profit"]
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else 0.0
        )
        metric.total_commission = prev["total_commission"] + deal["Commission"]
        metric.total_storage = prev["total_storage"] + deal["Storage"]
        metric.total_trades = prev["total_trades"] + (
            1
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else 0
        )

        return metric

