from typing import Annotated, Dict, List, Tuple,Type
import datetime
import pandas as pd

from src.account_metrics.model import MetricCalculator, MetricData
from .account_metric_by_day_data_model import AccountMetricDaily
import MT5Manager
from src.account_metrics.metrics.metric_utils import apply_groupby_mapping_of_metric_to_data

# TODO: Build translator of these MT5Manager.MTDeal.EnDealAction.DEAL_BUY to EnDealAction.DEAL_BUY
class AccountMetricDailyCalculator(MetricCalculator):
    input_class = ["Deal","History"]
    output_metric = AccountMetricDaily
    groupby_field = [k for k, v in output_metric.model_fields.items() if "groupby" in v.metadata]

    
    @classmethod
    def validate_data(cls,input_data: Dict[str, pd.DataFrame]):
        for key in cls.input_class:
            if key not in input_data.keys():
                print(f"Key {key} not in input data = {input_data.keys()}")
                raise ValueError("Invalid input data")
        if(input_data['Deal'].empty):
            return False
        return True
    
    @classmethod
    def calculate(cls,input_data: Dict[str, pd.DataFrame], current_metric:Dict[Type[MetricData], Dict[tuple, pd.Series]]) -> pd.DataFrame:
        if not cls.validate_data(input_data):
            return pd.DataFrame(columns=cls.output_metric.model_fields.keys())
        
        map_login_to_deals: Dict[Tuple[Annotated[int, "server"], Annotated[int, "login"]], pd.DataFrame] = apply_groupby_mapping_of_metric_to_data(cls.output_metric, input_data['Deal'])

        result: list[pd.Series] = []
        keys = list(map_login_to_deals.keys())

        for i in range(len(keys)):
            deals_of_login = map_login_to_deals[keys[i]]
            init_key_value = {k: keys[i][j] for j, k in enumerate(cls.groupby_field)}
            current_metric_of_login = current_metric.get(keys[i], pd.Series(cls.output_metric(**init_key_value).model_dump()))
            for index, deal in deals_of_login.iterrows():
                if deal["Deal"] <= current_metric_of_login["deal_id"]:
                    continue
                calculated_metric:AccountMetricDaily = cls.calculate_row(deal, current_metric_of_login, input_data['History'])
                new_row = pd.Series(calculated_metric.model_dump())
                result.append(new_row)
                current_metric_of_login = new_row

        return pd.DataFrame(result, columns=cls.output_metric.model_fields.keys())
    
    @classmethod
    def calculate_row(cls, deal: pd.Series, prev:pd.Series, history:pd.DataFrame) -> AccountMetricDaily:
        metric= cls.output_metric()
        comment = deal["Comment"] if isinstance(deal["Comment"], str) else deal["Comment"].decode()
        
        yesterday_history = history[
            (history["Date"] == pd.to_datetime(deal["Time"], unit="s").date() - datetime.timedelta(days=1))
            & (history["Login"] == deal["Login"])
        ]
        if len(yesterday_history) == 0:
            yesterday_history = pd.Series(
                {
                    "Login": deal["Login"],
                    "Balance": 0.0,
                    "ProfitEquity": 0.0,
                }
            )
        else:
            yesterday_history = yesterday_history.iloc[0]

        metric.server = deal["server"]
        metric.login = deal["Login"]
        metric.deal_id = deal["Deal"]
        metric.timestamp_server = deal["Time"]
        metric.timestamp_utc = deal["TimeUTC"]
        metric.date = pd.to_datetime(deal["Time"], unit="s").date()
        metric.balance = prev["balance"] + deal["Profit"]
        metric.max_balance_equity = max(yesterday_history["Balance"], yesterday_history["ProfitEquity"])
        metric.net_deposit = prev["net_deposit"] + (
            deal["Profit"]
            if deal["Action"] == MT5Manager.MTDeal.EnDealAction.DEAL_BALANCE and "initialize" not in comment
            else 0.0
        )
        metric.yesterday_net_deposit = (
            prev["net_deposit"] if pd.to_datetime(prev["date"]).date() < metric.date else prev["yesterday_net_deposit"]
        )
        metric.daily_net_deposit = metric.net_deposit - metric.yesterday_net_deposit
        metric.profit_loss = prev["profit_loss"] + (
            (deal["Profit"] + deal["Commission"] + deal["Storage"])
            if deal["Action"]
            not in [
                MT5Manager.MTDeal.EnDealAction.DEAL_BALANCE,
                MT5Manager.MTDeal.EnDealAction.DEAL_CREDIT,
                MT5Manager.MTDeal.EnDealAction.DEAL_CORRECTION,
                MT5Manager.MTDeal.EnDealAction.DEAL_SO_COMPENSATION,
            ]
            else 0.0
        )
        metric.yesterday_net_profit_loss = (
            prev["profit_loss"] if pd.to_datetime(prev["date"]).date() < metric.date else prev["yesterday_net_profit_loss"]
        )
        metric.daily_profit_loss = metric.profit_loss - metric.yesterday_net_profit_loss
        metric.last_open_trade_timestamp = (
            deal["Time"] if deal["Entry"] == MT5Manager.MTDeal.EnDealEntry.ENTRY_IN else prev["last_open_trade_timestamp"]
        )
        metric.trading_days = prev["trading_days"] + (
            1 if pd.to_datetime(prev["last_open_trade_timestamp"], unit="s") < pd.to_datetime(metric.last_open_trade_timestamp, unit="s") else 0
        )
        metric.yesterday_profitable_trading_days = (
            prev["profitable_trading_days"] if pd.to_datetime(prev["date"]).date() < metric.date else prev["yesterday_profitable_trading_days"]
        )
        metric.profitable_trading_days = metric.yesterday_profitable_trading_days + (1 if metric.daily_profit_loss > 0 else 0)
        metric.total_deposit = prev["total_deposit"] + (
            deal["Profit"]
            if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BALANCE]
            and deal["Profit"] > 0
            and "initialize" not in comment
            else 0.0
        )
        metric.total_withdrawal = prev["total_withdrawal"] + (
            deal["Profit"] if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BALANCE] and deal["Profit"] < 0 else 0.0
        )
        metric.count_trades = prev["count_trades"] + (1 if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BUY, MT5Manager.MTDeal.EnDealAction.DEAL_SELL] and deal["Entry"] in [MT5Manager.MTDeal.EnDealEntry.ENTRY_IN] else 0)
        metric.count_long_trades = prev["count_long_trades"] + (1 if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BUY] and deal["Entry"] in [MT5Manager.MTDeal.EnDealEntry.ENTRY_IN] else 0)
        metric.count_profit_trades = prev["count_profit_trades"] + (
            1
            if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BUY, MT5Manager.MTDeal.EnDealAction.DEAL_SELL]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            and deal["Profit"] > 0
            else 0
        )
        metric.count_loss_trades = prev["count_loss_trades"] + (
            1
            if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BUY, MT5Manager.MTDeal.EnDealAction.DEAL_SELL]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            and deal["Profit"] < 0
            else 0
        )
        metric.gross_profit = prev["gross_profit"] + (
            deal["Profit"]
            if deal["Action"]
            not in [
                MT5Manager.MTDeal.EnDealAction.DEAL_BALANCE,
                MT5Manager.MTDeal.EnDealAction.DEAL_CREDIT,
                MT5Manager.MTDeal.EnDealAction.DEAL_CORRECTION,
                MT5Manager.MTDeal.EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            and deal["Profit"] > 0
            else 0.0
        )
        metric.gross_loss = prev["gross_loss"] + (
            deal["Profit"]
            if deal["Action"]
            not in [
                MT5Manager.MTDeal.EnDealAction.DEAL_BALANCE,
                MT5Manager.MTDeal.EnDealAction.DEAL_CREDIT,
                MT5Manager.MTDeal.EnDealAction.DEAL_CORRECTION,
                MT5Manager.MTDeal.EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            and deal["Profit"] < 0
            else 0.0
        )
        metric.wins_ratio = metric.count_profit_trades / metric.count_trades if metric.count_trades > 0 else 0.0
        metric.losses_ratio = metric.count_loss_trades / metric.count_trades if metric.count_trades > 0 else 0.0
        metric.total_volume = prev["total_volume"] + (
            deal["Volume"]
            if deal["Action"] in [MT5Manager.MTDeal.EnDealAction.DEAL_BUY, MT5Manager.MTDeal.EnDealAction.DEAL_SELL]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            else 0.0
        )
        metric.best_trade = (
            deal["Profit"]
            if deal["Profit"] > prev["best_trade"]
            and deal["Action"]
            not in [
                MT5Manager.MTDeal.EnDealAction.DEAL_BALANCE,
                MT5Manager.MTDeal.EnDealAction.DEAL_CREDIT,
                MT5Manager.MTDeal.EnDealAction.DEAL_CORRECTION,
                MT5Manager.MTDeal.EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            else prev["best_trade"]
        )
        metric.worst_trade = (
            deal["Profit"]
            if deal["Profit"] < prev["worst_trade"]
            and deal["Action"]
            not in [
                MT5Manager.MTDeal.EnDealAction.DEAL_BALANCE,
                MT5Manager.MTDeal.EnDealAction.DEAL_CREDIT,
                MT5Manager.MTDeal.EnDealAction.DEAL_CORRECTION,
                MT5Manager.MTDeal.EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and deal["Entry"]
            in [MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_INOUT, MT5Manager.MTDeal.EnDealEntry.ENTRY_OUT_BY]
            else prev["worst_trade"]
        )
        metric.average_win = metric.gross_profit / metric.count_profit_trades if metric.count_profit_trades > 0 else 0.0
        metric.average_loss = metric.gross_loss / metric.count_loss_trades if metric.count_loss_trades > 0 else 0.0

        return metric
