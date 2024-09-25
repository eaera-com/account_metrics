from typing import List, Any, Dict, Type
import abc
import pandas as pd
from pydantic import BaseModel
        

class MetricCalculator(abc.ABC):
    @property
    def input_class(self) -> List[str]:
        raise NotImplementedError()
    
    @property
    def output_class(self) -> List[str]:
        raise NotImplementedError()
    
    @classmethod
    @abc.abstractmethod
    def calculate(self, input_data: Dict[str, pd.DataFrame], current_metric:Any) -> pd.DataFrame:
        raise NotImplementedError()
    
    @classmethod
    @abc.abstractmethod
    def validate_data(self, input_data: Dict[str, pd.DataFrame]):
        raise NotImplementedError()
    
class MetricData(BaseModel, abc.ABC):
    # TODO: enforce all variables are assigned in data_model
    class Meta:
        key_columns = []
        groupby = []
        does_stream_out = False
        calculator: Type[MetricCalculator] = None