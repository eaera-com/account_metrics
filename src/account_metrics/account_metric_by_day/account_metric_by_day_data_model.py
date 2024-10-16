import datetime

from  typing import Annotated
from ..metric_model import MetricData

class AccountMetricDaily(MetricData):
    server: Annotated[str, "key", "groupby"] = ""
    login: Annotated[int, "key", "groupby"] = 0
    date: Annotated[datetime.date, "key"] = datetime.date(1970, 1, 1)
    deal_id: int = 0
    timestamp_server: int = 0
    timestamp_utc: int = 0
    balance: float = 0.0
    max_balance_equity: float = 0.0
    net_deposit: float = 0.0
    yesterday_net_deposit: float = 0.0
    daily_net_deposit: float = 0.0
    profit_loss: float = 0.0
    yesterday_net_profit_loss: float = 0.0
    daily_profit_loss: float = 0.0
    # current_daily_drawdown: float = 0.0
    # mdd_threshold: float = 0.0
    # current_absolute_drawdown = 0.0
    last_open_trade_timestamp: int = 0
    trading_days: int = 0
    yesterday_profitable_trading_days: int = 0
    profitable_trading_days: int = 0
    total_deposit: float = 0.0
    total_withdrawal: float = 0.0
    count_trades: int = 0
    count_long_trades: int = 0
    count_profit_trades: int = 0
    count_loss_trades: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    wins_ratio: float = 0.0
    losses_ratio: float = 0.0
    total_volume: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0
    average_win: float = 0.0
    average_loss: float = 0.0
 
    class Meta(MetricData.Meta):
        kafka_num_consumers = 3
        key_columns = ["server", "login", "date"]
        groupby = ["server", "login"]
        groupby_update_format = ["server", "login"]