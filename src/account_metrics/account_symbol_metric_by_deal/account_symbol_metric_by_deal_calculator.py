from typing import Dict, Tuple, Annotated,Type
import pandas as pd

from src.account_metrics.model import MetricCalculator, MetricData
from .account_symbol_metric_by_deal_data_model import AccountSymbolMetricByDeal
import MT5Manager
from src.account_metrics.metrics.metric_utils import apply_groupby_mapping_of_metric_to_data

# TODO: Build translator of these MT5Manager.MTDeal.EnDealAction.DEAL_BUY to EnDealAction.DEAL_BUY
class AccountSymbolMetricByDealCalculator(MetricCalculator):
        
    input_class = ["Deal"]
    output_metric = AccountSymbolMetricByDeal
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
    def calculate(cls,input_data: Dict[str, pd.DataFrame],current_metric:Dict[Type[MetricData], Dict[tuple, pd.Series]]) -> pd.DataFrame:
        if not cls.validate_data(input_data):
            return pd.DataFrame(columns=cls.output_metric.model_fields.keys())
        
        map_login_to_deals: Dict[Tuple[Annotated[int, "server"], Annotated[int, "login"]], pd.DataFrame] = apply_groupby_mapping_of_metric_to_data(cls.output_metric, input_data['Deal'])


        result: list[pd.Series] = []
        keys = list(map_login_to_deals.keys())

        for i in range(len(keys)):
            deals_of_login = map_login_to_deals[keys[i]]
            init_key_value = {k: keys[i][j] for j, k in enumerate(cls.groupby_field)}
            current_metric_of_login = current_metric.get(keys[i], pd.Series(cls.output_metric(**init_key_value).model_dump()))
            for index, deal in deals_of_login.iterrows():
                if deal["Deal"] <= current_metric_of_login["deal_id"]:
                    continue
                calculated_metric:AccountSymbolMetricByDeal = cls.calculate_row(deal, current_metric_of_login)
                new_rows = pd.Series(calculated_metric.model_dump())
                result.append(new_rows)
                current_metric_of_login = new_rows

        return pd.DataFrame(result, columns=cls.output_metric.model_fields.keys())
        
    @classmethod
    def calculate_row(cls,deal:pd.Series, prev:pd.Series) -> AccountSymbolMetricByDeal:
        metric = cls.output_metric()

        metric.server = deal["server"]
        metric.login = deal["Login"]
        metric.deal_id = deal["Deal"]
        metric.deal_profit = (
            deal["Profit"]
            if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BUY, MT5Manager.MTDeal.EnDealAction.DEAL_SELL]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            else 0.0
        )
        metric.timestamp_server = deal["Time"]
        metric.timestamp_utc = deal["TimeUTC"]
        metric.symbol = deal["Symbol"]
        metric.total_profit = prev["total_profit"] + (
            deal["Profit"]
            if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BUY, MT5Manager.MTDeal.EnDealAction.DEAL_SELL]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            else 0.0
        )
        metric.total_commission = prev["total_commission"] + deal["Commission"]
        metric.total_storage = prev["total_storage"] + deal["Storage"]
        metric.total_trades = prev["total_trades"] + (
            1
            if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BUY, MT5Manager.MTDeal.EnDealAction.DEAL_SELL]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            else 0
        )

        return metric

