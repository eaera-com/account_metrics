from typing import List, Annotated
import datetime
from src.account_metrics.model import MetricData

# MT5 History Deal
# TODO: INCOPORATED INTO DATA MODEL AND OTHER QUERY FROM CLICKHOUSE (use same format for all data models)
# TODO: Add server and timestamp_server to all data models
class MT5DealDaily(MetricData):
    timestamp_utc: Annotated[int, "key"]
    Login: Annotated[int, "key"]
    Group: str
    Datetime: int
    Balance: float
    ProfitEquity: float
    Date: datetime.date
    server: str
    timestamp_server: int
    class Meta(MetricData.Meta):
        key_columns = ["Login","timestamp_utc"]


