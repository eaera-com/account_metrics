from  typing import Annotated

from account_metrics.metric_model import MetricData

# TODO: INCOPORATED INTO DATA MODEL AND OTHER QUERY FROM CLICKHOUSE (use same format for all data models)
class MT5Deal(MetricData):
    Action: int = 0
    Comment: str = ""
    Commission: float = 0.0
    ContractSize: float = 0.0
    server: str = ""
    Deal: Annotated[int, "key"] = 0
    deal_id: Annotated[int, "key"] = 0
    Dealer: Annotated[int, "groupby"] = 0
    Digits: int = 0
    DigitsCurrency: int = 0
    Entry: int = 0
    ExpertID: int = 0
    ExternalID: str = ""
    Fee: float = 0.0
    Flags: int = 0
    Gateway: str = ""
    Login: int = 0
    login: int
    MarketAsk: float = 0.0
    MarketBid: float = 0.0
    MarketLast: float = 0.0
    ModificationFlags: int = 0
    Order: int = 0
    PositionID: int = 0
    Price: float = 0.0
    PriceGateway: float = 0.0
    PricePosition: float = 0.0
    PriceSL: float = 0.0
    PriceTP: float = 0.0
    Profit: float = 0.0
    ProfitRaw: float = 0.0
    RateMargin: float = 0.0
    RateProfit: float = 0.0
    Reason: str = ""
    Storage: float = 0.0
    Symbol: str = ""
    TickSize: float = 0.0
    TickValue: float = 0.0
    Time: int = 0
    timestamp_server: int = 0
    timestamp_utc: int = 0
    TimeUTC: int = 0
    TimeMsc: int = 0
    Value: float = 0.0
    Volume: int = 0
    VolumeClosed: int = 0
    VolumeClosedExt: int = 0
    VolumeExt: int = 0

    class Meta(MetricData.Meta):
        sharding_columns = ["Deal"]