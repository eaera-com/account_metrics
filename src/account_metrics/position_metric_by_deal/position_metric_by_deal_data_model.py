from  typing import Annotated
from account_metrics.metric_model import MetricData

class PositionMetricByDeal(MetricData):
    server: Annotated[str, "key", "groupby"] = ""
    action: int = 0
    comment: str = ""
    commission: float = 0.0
    deal_id: int = 0
    digits: int = 0
    digits_currency: int = 0
    login: int = 0
    position_id: Annotated[int, "key", "groupby"] = 0
    price: float = 0.0
    price_position: float = 0.0
    price_sl: float = 0.0
    price_tp: float = 0.0
    profit: float = 0.0
    profit_raw: float = 0.0
    rate_margin: float = 0.0
    storage: float = 0.0
    symbol: str = ""
    timestamp_utc: int = 0
    timestamp_server: int = 0
    timestamp_open: int = 0
    timestamp_open_server: int = 0
    volume: int = 0
    volume_closed: int = 0
    volume_ext: int = 0
    volume_closed_ext: int = 0
    volume_remaining: int = 0
    volume_remaining_ext: int = 0
    net_profit: float = 0.0
    
    class Meta(MetricData.Meta):
        kafka_num_consumers = 3
        key_columns = ["server", "position_id"]
        # TODO: fix PositionID permernantly
        groupby = ["server", "PositionID"]