from typing import Annotated, List, Any
import abc
import pandas as pd
from pydantic import BaseModel

class MetricCalculator(abc.ABC):
    input_class:  Annotated[Any, "MetricData"] = None 
    output_metric: Annotated[Any, "MetricData"] = None 
    additional_data: List[Annotated[Any, "MetricData"]] = None
    groupby_field : List[Any] = None
    metric_runner: Annotated[Any, "MetricRunner"] = None

    
    @classmethod    
    @abc.abstractmethod
    def calculate(cls, input_data: pd.DataFrame, current_metric:Any) -> pd.DataFrame:
        raise NotImplementedError()
    
    @classmethod
    def set_metric_runner(cls, metric_runner: Any):
        cls.metric_runner = metric_runner
    @classmethod
    def get_metric_runner(cls):
        if cls.metric_runner is None:
            raise ValueError("Metric runner is not set")
        return cls.metric_runner
        
class MetricData(BaseModel, abc.ABC):
    # TODO: enforce all variables are assigned in data_model
    class Meta:
        sharding_columns = []
        groupby = []
        does_stream_out = False