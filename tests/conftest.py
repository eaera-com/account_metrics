from typing import Dict, Type, Any
import pytest
from pydantic.alias_generators import to_snake
import pandas as pd
import os
import datetime

from account_metrics.datastore import Datastore
from account_metrics.metric_model import MetricData

from account_metrics.account_metric_by_day import AccountMetricDaily
from account_metrics.account_metric_by_deal import  AccountMetricByDeal
from account_metrics.account_symbol_metric_by_deal  import AccountSymbolMetricByDeal
from account_metrics.position_metric_by_deal import PositionMetricByDeal
from account_metrics.mt5_deal import MT5Deal
from account_metrics.mt5_deal_daily import MT5DealDaily
        

TEST_METRICS = [AccountMetricDaily, AccountMetricByDeal, AccountSymbolMetricByDeal, PositionMetricByDeal, MT5Deal,MT5DealDaily]
# TODO: find a better way to get the path.
TEST_DATAFRAME_PATH = {MT5Deal:  os.path.abspath("tests/test_data/mt5_deal.csv"),
                       MT5DealDaily: os.path.abspath("tests/test_data/mt5_deal_daily.csv"),
                       AccountMetricDaily: os.path.abspath("tests/test_data/account_metric_daily.csv"),
                       AccountMetricByDeal: os.path.abspath("tests/test_data/account_metric_by_deal.csv"),
                       AccountSymbolMetricByDeal: os.path.abspath("tests/test_data/account_symbol_metric_by_deal.csv"),
                       PositionMetricByDeal: os.path.abspath("tests/test_data/position_metric_by_deal.csv")}

def get_test_metric_name(metric:MetricData,test_name:str):
    return f"{test_name}_{to_snake(metric.__name__)}"

def get_metric_from_csv(metric:MetricData,path:str):
    date_columns = [field_name for field_name, field in metric.model_fields.items() if field.annotation == datetime.date]
    df = pd.read_csv(path, dtype=extract_type_mapping(metric),parse_dates=date_columns)
    # Convert string columns with NA values to empty strings
    string_columns = df.select_dtypes(include=['string']).columns
    df[string_columns] = df[string_columns].fillna('')
    df = setup_date_column_type(df,metric)
    return df

def extract_type_mapping(metric: MetricData):
    type_mapping = {}
    for field_name, field in metric.model_fields.items():
        if field.annotation in [int, 'int32', 'int64']:
            type_mapping[field_name] = 'int64'  # Use nullable integer type
        elif field.annotation == float:
            type_mapping[field_name] = 'float64'
        elif field.annotation in [str, 'string']:
            type_mapping[field_name] = 'string'
        else:
            type_mapping[field_name] = 'object'
    return type_mapping

def setup_string_column_type(df:pd.DataFrame, metric:MetricData):
    for field_name, field in metric.model_fields.items():
        if field.annotation in [str, 'string']:
            df[field_name] = df[field_name].astype('string')
    return df

def setup_date_column_type(df:pd.DataFrame, metric:MetricData):
    for field_name, field in metric.model_fields.items():
        if field.annotation == datetime.date:
            df[field_name] = pd.to_datetime(df[field_name]).dt.date
    return df

def strip_quotes_from_string_columns(df:pd.DataFrame):
    for col in df.select_dtypes(include=['object','string']).columns:
        df[col] = df[col].apply(lambda x: x[2:-1] if isinstance(x, str) and x.startswith("b'") and x.endswith("'") 
                                else x.strip("'") if isinstance(x, str) else x)
        df[col] = df[col].astype('string')  
    return df
    
@pytest.fixture
def get_test_name(request):
    return request.node.name

class MockDatastore(Datastore):
    def __init__(self,data:Dict[Type[MetricData],Any]):
        self.data = data
        self.timestamps = 0
        
    
    def contains(self,metric_data:Type[MetricData]) -> bool:
        return metric_data in self.data

    def get(self,metric_data:Type[MetricData],additional_keys:Dict[str,Any]=None) -> pd.DataFrame:
        if self.timestamps != 0:
            return self.data[metric_data][(self.data[metric_data]["timestamp_utc"] < self.timestamps)]
        if metric_data == PositionMetricByDeal :
            return self.data[metric_data][self.data[metric_data]["position_id"] == additional_keys["position_id"]]
        return self.data[metric_data]
    
    def put(self,metric_data:Type[MetricData],data:Any,additional_keys:Dict[Type[MetricData],Any]=None):
        self.data[metric_data] = data