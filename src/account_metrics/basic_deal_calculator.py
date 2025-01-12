from typing import Annotated, Dict, Any, Tuple, Type
import pandas as pd
import abc

from account_metrics.metric_model import MetricData,MetricCalculator
from account_metrics.metric_utils import apply_groupby_mapping_of_metric_to_data

class BasicDealMetricCalculator(MetricCalculator,abc.ABC):
    
    @classmethod
    def calculate(cls,input_data:pd.DataFrame) -> pd.DataFrame:
        if (input_data is None or input_data.empty):
            return pd.DataFrame(columns=cls.output_metric.model_fields.keys())
        map_deals_by_groupby: Dict[Tuple[Annotated[int, "server"], Annotated[int, "login"]], pd.DataFrame] = apply_groupby_mapping_of_metric_to_data(cls.output_metric, input_data)

        result: list[pd.Series] = []
        keys = list(map_deals_by_groupby.keys())
                
        for i in range(len(keys)):
            deals_of_login = map_deals_by_groupby[keys[i]]
            current_metric_of_login = cls.get_metric_runner().get_datastore(cls.output_metric).get_latest_row({"login":keys[i][1]})
            for index, deal in deals_of_login.iterrows():
                if current_metric_of_login is not None and deal["Deal"] <= current_metric_of_login["deal_id"]:
                    continue
                calculated_metric, carry_data = cls.calculate_row(deal, {"current_metric":current_metric_of_login})
                new_row = pd.Series(calculated_metric.model_dump())
                result.append(new_row)
                current_metric_of_login = new_row

        return pd.DataFrame(result, columns=cls.output_metric.model_fields.keys())
    
    @abc.abstractmethod
    def calculate_row(cls,deal:pd.Series) -> MetricData:
        raise NotImplementedError()
