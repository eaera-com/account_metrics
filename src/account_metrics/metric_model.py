from typing import List, Any, Dict, Type
import abc
import pandas as pd
from pydantic import BaseModel
from abc import abstractmethod

class MetricCalculator(abc.ABC):
    input_class: Type[Any] = None 
    output_metric: Type[Any] = None 
    additional_data: List[Any] = None
    groupby_field : List[Any] = None

    
    @classmethod    
    @abc.abstractmethod
    def calculate(cls, input_data: pd.DataFrame, current_metric:Any) -> pd.DataFrame:
        raise NotImplementedError()
    
    @classmethod
    def get_metric_runner(cls):
        raise NotImplementedError()
        
class MetricData(BaseModel, abc.ABC):
    # TODO: enforce all variables are assigned in data_model
    class Meta:
        key_columns = []
        groupby = []
        does_stream_out = False
        calculator: Type[MetricCalculator] = None