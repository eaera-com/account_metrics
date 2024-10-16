from typing import Annotated, Dict, Any, Tuple, Type
import pandas as pd

from account_metrics.account_metric_by_day.account_metric_by_day_data_model import AccountMetricDaily
from account_metrics.datastore import Datastore
from account_metrics.metric_model import MetricData,MetricCalculator
from account_metrics.metric_utils import apply_groupby_mapping_of_metric_to_data
from account_metrics.mt5_deal_daily.mt5_deal_daily_data_model import MT5DealDaily

class BasicDealMetricCalculator(MetricCalculator):
    def __init__(self,datastore:Datastore):
            self.datastore = datastore
        
    def get_dataframe_from_datastore(self,metric_data:Type[MetricData]):
        assert metric_data in self.__class__.addtional_data
        return self.datastore.get(metric_data)
    
    def get_row_from_datastore(self,metric_data:Type[MetricData],additional_keys:Dict[Type[MetricData],Any]):  
        """
        Retrieve a row from the datastore based on the provided metric data and additional keys.

        Args:
            metric_data (Type[MetricData]): The type of metric data to retrieve.
            additional_keys (Dict[Type[MetricData], Any]): Additional keys for filtering the datastore query.

        Returns:
            pd.Series: The retrieved row as a pandas Series. Returns a default metric row if no data is found.
            In case of multiple rows, the last row is returned.
        """
        assert metric_data in self.__class__.addtional_data
        for key in additional_keys:
            assert key in metric_data.model_fields
        df =  self.datastore.get(metric_data,additional_keys)
        if df.empty:
            return pd.Series(self.__class__.output_metric(**additional_keys).model_dump())
        return df.iloc[-1]
    
    def calculate(self,input_data:pd.DataFrame) -> pd.DataFrame:
        if (input_data is None or input_data.empty):
            return pd.DataFrame(columns=self.__class__.output_metric.model_fields.keys())
        
        map_deals_by_groupby: Dict[Tuple[Annotated[int, "server"], Annotated[int, "login"]], pd.DataFrame] = apply_groupby_mapping_of_metric_to_data(self.__class__.output_metric, input_data)

        result: list[pd.Series] = []
        keys = list(map_deals_by_groupby.keys())
        
        additional_df = {}
        for metric in self.__class__.addtional_data:
            if metric != self.__class__.output_metric:
                additional_df[metric] = self.get_dataframe_from_datastore(metric)
                
        for i in range(len(keys)):
            deals_of_login = map_deals_by_groupby[keys[i]]
        
            # TODO: make the key[i] less hacky
            additional_keys = {"server":keys[i][0],"login":keys[i][1]}
            current_metric_of_login:pd.Series = self.get_row_from_datastore(self.__class__.output_metric,additional_keys)
            for index, deal in deals_of_login.iterrows():
                if deal["Deal"] <= current_metric_of_login["deal_id"]:
                    continue
                calculated_metric:AccountMetricDaily = self.calculate_row(deal, current_metric_of_login, additional_df)
                new_row = pd.Series(calculated_metric.model_dump())
                result.append(new_row)
                current_metric_of_login = new_row

        return pd.DataFrame(result, columns=self.__class__.output_metric.model_fields.keys())
    
    def calculate_row(self,deal:pd.Series,current_metric:pd.Series,additional_df:Dict[MetricData,pd.DataFrame]) -> AccountMetricDaily:
        raise NotImplementedError()
