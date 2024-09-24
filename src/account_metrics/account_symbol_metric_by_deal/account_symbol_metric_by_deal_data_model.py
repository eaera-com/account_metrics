from  typing import Annotated
from src.account_metrics.model import MetricData

class AccountSymbolMetricByDeal(MetricData):
    server: Annotated[str, "key", "groupby"] = ""
    login: Annotated[int, "key", "groupby"] = 0
    deal_id: Annotated[int, "key"] = 0
    deal_profit: float = 0.0
    timestamp_server: int = 0
    timestamp_utc: int = 0
    symbol: str = ""
    total_profit: float = 0.0
    total_commission: float = 0.0
    total_storage: float = 0.0
    total_trades: int = 0

    class Meta(MetricData.Meta):
        kafka_num_consumers = 3
        key_columns = ["server", "login","deal_id"]
        groupby = ["server", "login"]