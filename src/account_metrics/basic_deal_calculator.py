from typing import Annotated, Dict, Any, Tuple, Type
import pandas as pd
import abc

from account_metrics.metric_model import MetricData,MetricCalculator
from account_metrics.metric_utils import apply_groupby_mapping_of_metric_to_data

class BasicDealMetricCalculator(MetricCalculator,abc.ABC):
        
    # def get_dataframe_from_datastore(self,metric_data:Type[MetricData]):
    #     assert metric_data in self.__class__.additional_data
    #     return self.datastore.get(metric_data)
    
    # def get_row_from_datastore(self,metric_data:Type[MetricData],additional_keys:Dict[Type[MetricData],Any]):  
    #     """
    #     Retrieve a row from the datastore based on the provided metric data and additional keys.

    #     Args:
    #         metric_data (Type[MetricData]): The type of metric data to retrieve.
    #         additional_keys (Dict[Type[MetricData], Any]): Additional keys for filtering the datastore query.

    #     Returns:
    #         pd.Series: The retrieved row as a pandas Series. Returns a default metric row if no data is found.
    #         In case of multiple rows, the last row is returned.
    #     """
    #     assert metric_data in self.__class__.additional_data
    #     for key in additional_keys:
    #         assert key in metric_data.model_fields, f"{key} is not a field of {metric_data}"
    #     df =  self.datastore.get(metric_data,additional_keys)
    #     if df.empty:
    #         return pd.Series(self.__class__.output_metric(**additional_keys).model_dump())
    #     return df.iloc[-1]
    
    
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
                if deal["Deal"] <= current_metric_of_login["deal_id"]:
                    continue
                calculated_metric:MetricData = cls.calculate_row(deal, current_metric_of_login)
                new_row = pd.Series(calculated_metric.model_dump())
                result.append(new_row)
                current_metric_of_login = new_row

        return pd.DataFrame(result, columns=cls.output_metric.model_fields.keys())
    
    @abc.abstractmethod
    def calculate_row(cls,deal:pd.Series) -> MetricData:
        raise NotImplementedError()
