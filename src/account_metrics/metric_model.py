from typing import Annotated, List, Any, Dict, Type
import abc
import pandas as pd
from pydantic import BaseModel
from abc import abstractmethod

class MetricCalculator(abc.ABC):
    input_class:  Annotated[Any, "MetricData"] = None 
    output_metric: Annotated[Any, "MetricData"] = None 
    additional_data: List[Annotated[Any, "MetricData"]] = None
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