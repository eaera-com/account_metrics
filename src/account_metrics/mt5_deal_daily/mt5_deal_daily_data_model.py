from typing import List, Annotated
import datetime
from account_metrics.metric_model import MetricData

# MT5 History Deal
# TODO: INCOPORATED INTO DATA MODEL AND OTHER QUERY FROM CLICKHOUSE (use same format for all data models)
# TODO: Add server and timestamp_server to all data models
class MT5DealDaily(MetricData):
    timestamp_utc: Annotated[int, "key"]
    Login: Annotated[int, "key"]
    Group: str = ""
    Datetime: int = 0
    Balance: float = 0.0
    ProfitEquity: float = 0.0
    Date: Annotated[datetime.date, "key"] = datetime.date(1970, 1, 1)
    server: str = ""
    timestamp_server: int = 0
    class Meta(MetricData.Meta):
        key_columns = ["Login","timestamp_utc"]


