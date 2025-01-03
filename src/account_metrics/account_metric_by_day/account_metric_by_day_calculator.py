import datetime
from typing import Union
import pandas as pd

from .account_metric_by_day_data_model import AccountMetricDaily

from account_metrics.mt5_deal.mt5_deal_data_model import MT5Deal
from account_metrics.mt5_deal_daily.mt5_deal_daily_data_model import MT5DealDaily
from account_metrics.mt_deal_enum import EnDealAction,EnDealEntry
from account_metrics.basic_deal_calculator import BasicDealMetricCalculator

class AccountMetricDailyCalculator(BasicDealMetricCalculator):
    input_class = MT5Deal
    additional_data = [MT5DealDaily,AccountMetricDaily]
    output_metric = AccountMetricDaily
    groupby_field = [k for k, v in output_metric.model_fields.items() if "groupby" in v.metadata]
    
    @classmethod
    def calculate_row(cls, deal: pd.Series, prev: Union[AccountMetricDaily, None]) -> AccountMetricDaily:
        comment = deal["Comment"] if isinstance(deal["Comment"], str) else deal["Comment"].decode()
        action = deal["Action"] if isinstance(deal["Action"], EnDealAction) else EnDealAction(deal["Action"])
        entry = deal["Entry"] if isinstance(deal["Entry"], EnDealEntry) else EnDealEntry(deal["Entry"])
        is_initialize = "initialize" in comment

        yesterday_history = cls.get_metric_runner().get_datastore(MT5DealDaily).get_row_by_timestamp({"Login":deal.login},
                                                                                                     pd.to_datetime(deal["Time"], unit="s").date() - datetime.timedelta(days=1),
                                                                                                     timestamp_column="Date")
        # TODO: double check the logic of initialize what happen if yesterday_history exists when initialize
        if (is_initialize or "Deposit" in comment) and yesterday_history is None:
            yesterday_history = pd.Series(
            {
                "Login": deal["Login"],
                "Balance": 0.0,
                "ProfitEquity":0.0,
            }
        )
            
        if (is_initialize or "Deposit" in comment) and prev is None:
            prev = cls.output_metric()
            prev.deal_id = deal.deal_id

        #TODO: throw a defined error to catch and handle later
        assert yesterday_history is not None, "No history for {deal['Login']} on {pd.to_datetime(deal['Time'], unit='s').date() - datetime.timedelta(days=1)}"
        
        metric= cls.output_metric()

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
            if action == EnDealAction.DEAL_BALANCE and not is_initialize
            else 0.0
        )
        metric.yesterday_net_deposit = (
            prev["net_deposit"] if pd.to_datetime(prev["date"]).date() < metric.date else prev["yesterday_net_deposit"]
        )
        metric.daily_net_deposit = metric.net_deposit - metric.yesterday_net_deposit
        metric.profit_loss = prev["profit_loss"] + (
            (deal["Profit"] + deal["Commission"] + deal["Storage"])
            if action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_CORRECTION,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            else 0.0
        )
        metric.yesterday_net_profit_loss = (
            prev["profit_loss"] if pd.to_datetime(prev["date"]).date() < metric.date else prev["yesterday_net_profit_loss"]
        )
        metric.daily_profit_loss = metric.profit_loss - metric.yesterday_net_profit_loss
        metric.last_open_trade_timestamp = (
            deal["Time"] if entry == EnDealEntry.ENTRY_IN else prev["last_open_trade_timestamp"]
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
            if action in [EnDealAction.DEAL_BALANCE]
            and deal["Profit"] > 0
            and not is_initialize
            else 0.0
        )
        metric.total_withdrawal = prev["total_withdrawal"] + (
            deal["Profit"] if action in [EnDealAction.DEAL_BALANCE] and deal["Profit"] < 0 else 0.0
        )
        metric.count_trades = prev["count_trades"] + (1 if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL] and entry in [EnDealEntry.ENTRY_IN] else 0)
        metric.count_long_trades = prev["count_long_trades"] + (1 if action in [EnDealAction.DEAL_BUY] and entry in [EnDealEntry.ENTRY_IN] else 0)
        metric.count_profit_trades = prev["count_profit_trades"] + (
            1
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            and deal["Profit"] > 0
            else 0
        )
        metric.count_loss_trades = prev["count_loss_trades"] + (
            1
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            and deal["Profit"] < 0
            else 0
        )
        metric.gross_profit = prev["gross_profit"] + (
            deal["Profit"]
            if action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_CORRECTION,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            and deal["Profit"] > 0
            else 0.0
        )
        metric.gross_loss = prev["gross_loss"] + (
            deal["Profit"]
            if action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_CORRECTION,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            and deal["Profit"] < 0
            else 0.0
        )
        metric.wins_ratio = metric.count_profit_trades / metric.count_trades if metric.count_trades > 0 else 0.0
        metric.losses_ratio = metric.count_loss_trades / metric.count_trades if metric.count_trades > 0 else 0.0
        metric.total_volume = prev["total_volume"] + (
            deal["Volume"]
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else 0.0
        )
        metric.best_trade = (
            deal["Profit"]
            if deal["Profit"] > prev["best_trade"]
            and action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_CORRECTION,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else prev["best_trade"]
        )
        metric.worst_trade = (
            deal["Profit"]
            if deal["Profit"] < prev["worst_trade"]
            and action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_CORRECTION,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else prev["worst_trade"]
        )
        metric.average_win = metric.gross_profit / metric.count_profit_trades if metric.count_profit_trades > 0 else 0.0
        metric.average_loss = metric.gross_loss / metric.count_loss_trades if metric.count_loss_trades > 0 else 0.0

        return metric
