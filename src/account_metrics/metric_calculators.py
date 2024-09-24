from typing import Dict, Type
import pandas as pd
from pydantic import BaseModel
from typing import List, Any
import abc

from .account_metric_by_day import AccountMetricDailyCalculator,AccountMetricDaily
from .account_metric_by_deal import AccountMetricByDealCalculator,AccountMetricByDeal
from .account_symbol_metric_by_deal import AccountSymbolMetricByDealCalculator,AccountSymbolMetricByDeal
from .position_metric_by_deal import PositionMetricByDealCalculator,PositionMetricByDeal
from .mt5_deal import MT5Deal
from .mt5_deal_daily import MT5DealDaily
from .metric_utils import decode_string_binary_column


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

@staticmethod
def identityFromDataframeCalculator(metric_class:MetricData, input_key: str) -> MetricCalculator:
    class IdentityCalculator(MetricCalculator):
        input_class = str
        key = input_key
        output_metric = metric_class
        @classmethod
        def calculate(cls,input_data: Dict[str, pd.DataFrame],  current_metric:Dict[Type[MetricData], Dict[tuple, pd.Series]]) -> pd.DataFrame:
            
            if not cls.validate_data(input_data):
                return pd.DataFrame()
            
            # Get all pydantic fields of MT5Deal with string type annotation
            data = input_data[cls.key]
            decode_string_binary_column(cls.output_metric, data)

            return data
        
        @classmethod
        def validate_data(cls,input_data: Dict[str, pd.DataFrame]) -> bool:
            return cls.key in input_data.keys() and not input_data[cls.key].empty
    return IdentityCalculator

# Define the mapping between metric data and metric calculator

METRIC_CALCULATORS:Dict[MetricData,MetricCalculator] = \
                    {
                        AccountMetricByDeal: AccountMetricByDealCalculator,
                        AccountMetricDaily: AccountMetricDailyCalculator,
                        AccountSymbolMetricByDeal: AccountSymbolMetricByDealCalculator,
                        PositionMetricByDeal: PositionMetricByDealCalculator,
                        MT5Deal: identityFromDataframeCalculator(MT5Deal,"Deal"),
                        MT5DealDaily: identityFromDataframeCalculator(MT5DealDaily,"History"),
                    }
