import pandas as pd
from typing import Type, Dict

from account_metrics.metric_model import MetricData
#  ------------------------------------------------- Helper function for metrics --------------------------------------------

# (MetricData.Meta.groupby) -> metric_row (1:1 mapping)
@staticmethod
def apply_groupby_mapping_to_metric(metric: Type[MetricData], current_metric: pd.DataFrame) -> Dict[tuple, pd.Series]: 
    map_groupby_to_current_metric:dict[tuple, pd.Series] = {}

    # TODO: Replace all groupby_field with Meta.groupby
    groupby_field = [k for k, v in metric.model_fields.items() if "groupby" in v.metadata]
    for index, row in current_metric.iterrows():
        key = tuple([row[f] for f in groupby_field])
        map_groupby_to_current_metric[key] = row
    return map_groupby_to_current_metric

# TODO: check if we could better here
# (MetricData.Meta.groupby) -> deal_rows (1:n mapping)
@staticmethod
def apply_groupby_mapping_of_metric_to_data(metric: Type[MetricData], data: pd.DataFrame) -> Dict[tuple, pd.Series]: 
    map_groupby_to_current_metric:dict[tuple, pd.Series] = {}
    deals_grouped = data.groupby(metric.Meta.groupby)
    for g in deals_grouped:
        map_groupby_to_current_metric[g[0]] = g[1].sort_values(["Time", "Deal"])
    return map_groupby_to_current_metric

@staticmethod
def decode_string_binary_column(metric: MetricData, data: pd.DataFrame):
    string_fields = [field for field, field_info in metric.__annotations__.items() if field_info == str]
    
    for field in string_fields:
        if data[field].apply(lambda x: isinstance(x, bytes)).any():
            data[field] = data[field].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        data[field] = data[field].astype(str)