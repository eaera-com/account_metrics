from typing import List, Any, Dict, Type
import abc
import pandas as pd
from pydantic import BaseModel
        

class MetricCalculator(abc.ABC):
    @property
    def input_class(self) -> List[str]:
        raise NotImplementedError()
    
    @property
    def output_metric(self) -> List[str]:
        raise NotImplementedError()
    
    @abc.abstractmethod
    def calculate(self, input_data: pd.DataFrame, current_metric:Any) -> pd.DataFrame:
        raise NotImplementedError()
    
    @abc.abstractmethod
    def get_dataframe_from_datastore(self,metric_data:Any,additional_keys:Any=None):
        raise NotImplementedError()
    
    @abc.abstractmethod
    def get_row_from_datastore(self,metric_data:Any,additional_keys:Any=None):
        raise NotImplementedError()
    
class MetricData(BaseModel, abc.ABC):
    # TODO: enforce all variables are assigned in data_model
    class Meta:
        key_columns = []
        groupby = []
        does_stream_out = False
        calculator: Type[MetricCalculator] = None