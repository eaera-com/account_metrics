import datetime
from typing import Dict
import pandas as pd


from .account_metric_by_deal_data_model import AccountMetricByDeal

from account_metrics.metric_model import MetricData
from account_metrics.mt_deal_enum import EnDealAction, EnDealEntry
from account_metrics.mt5_deal import MT5Deal
from account_metrics.mt5_deal_daily import MT5DealDaily
from account_metrics.datastore import Datastore
from account_metrics.basic_deal_calculator import BasicDealMetricCalculator

class AccountMetricByDealCalculator(BasicDealMetricCalculator):

    input_class = MT5Deal
    additional_data = [MT5DealDaily,AccountMetricByDeal]
    output_metric = AccountMetricByDeal
    groupby_field = [k for k, v in output_metric.model_fields.items() if "groupby" in v.metadata]
    
    @classmethod
    def calculate_row(cls,deal:pd.Series,prev:AccountMetricByDeal) ->AccountMetricByDeal:
        metric = cls.output_metric()
        yesterday_history: pd.Series = cls.get_metric_runner().get_datastore(MT5DealDaily).get_row_by_timestamp(deal.login,
                                                                                                     pd.to_datetime(deal["Time"], unit="s").date() - datetime.timedelta(days=1),
                                                                                                     timestamp_column="Date")
        
        comment = deal["Comment"] if isinstance(deal["Comment"], str) else deal["Comment"].decode()
        action = deal["Action"] if isinstance(deal["Action"], EnDealAction) else EnDealAction(deal["Action"])
        entry = deal["Entry"] if isinstance(deal["Entry"], EnDealEntry) else EnDealEntry(deal["Entry"])
        
        metric.initial_deposit = (
            deal["Profit"]
            if action == EnDealAction.DEAL_BALANCE and comment.startswith("initialize") 
            else prev["initial_deposit"]
        )
        metric.program_id = (
            comment.split()[2][3:] 
            if "initialize" in comment and len(comment.split()) >= 3 and len(comment.split()[2]) >= 3
            else prev["program_id"]
        )


        metric.server = deal["server"]
        metric.login = deal["Login"]
        metric.deal_id = deal["Deal"]
        metric.deal_profit = (
            deal["Profit"]
            if action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_CORRECTION,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            else 0
        )
        metric.timestamp_server = deal["Time"]
        metric.timestamp_utc = deal["TimeUTC"]
        metric.date = pd.to_datetime(deal["Time"], unit="s").date()
        metric.balance = prev["balance"] + deal["Profit"] + deal["Commission"] + deal["Storage"]
        metric.max_balance_equity = max(yesterday_history["Balance"], yesterday_history["ProfitEquity"])
        metric.net_deposit = prev["net_deposit"] + (
            deal["Profit"]
            if action == EnDealAction.DEAL_BALANCE and "initialize" not in comment
            else 0.0
        )
        metric.yesterday_net_deposit = (
            prev["net_deposit"] if pd.to_datetime(prev["date"]).date() < metric.date else prev["yesterday_net_deposit"]
        )
        metric.daily_net_deposit = metric.net_deposit - metric.yesterday_net_deposit
        metric.net_profit = deal["Profit"] + deal["Commission"] + deal["Storage"]
        metric.profit_loss = prev["profit_loss"] + (
            metric.net_profit
            if action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_CORRECTION,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            else 0
        )
        metric.profit_gain = metric.profit_loss / metric.initial_deposit * 100 if metric.initial_deposit > 0 else 0.0
        metric.yesterday_net_profit_loss = (
            prev["profit_loss"] if pd.to_datetime(prev["date"]).date() < metric.date else prev["yesterday_net_profit_loss"]
        )
        metric.daily_profit_loss = metric.profit_loss - metric.yesterday_net_profit_loss
        metric.last_open_trade_timestamp = (
            deal["Time"]
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry == EnDealEntry.ENTRY_IN
            else prev["last_open_trade_timestamp"]
        )
        metric.trading_days = prev["trading_days"] + (
            1
            if pd.to_datetime(prev["last_open_trade_timestamp"], unit="s").date()
            < pd.to_datetime(metric.last_open_trade_timestamp, unit="s").date()
            else 0
        )
        metric.yesterday_profitable_trading_days = (
            prev["profitable_trading_days"] if pd.to_datetime(prev["date"]).date() < metric.date else prev["yesterday_profitable_trading_days"]
        )
        metric.profitable_trading_days = metric.yesterday_profitable_trading_days + (1 if metric.daily_profit_loss > 0 else 0)
        metric.yesterday_sum_profit_on_profitable_trading_days = (
            prev["sum_profit_on_profitable_trading_days"]
            if pd.to_datetime(prev["date"]).date() < metric.date
            else prev["yesterday_sum_profit_on_profitable_trading_days"]
        )
        metric.sum_profit_on_profitable_trading_days = (
            (metric.yesterday_sum_profit_on_profitable_trading_days + metric.daily_profit_loss)
            if metric.daily_profit_loss > 0
            else metric.yesterday_sum_profit_on_profitable_trading_days
        )
        metric.yesterday_max_profit_on_profitable_trading_days = (
            prev["max_profit_on_profitable_trading_days"]
            if pd.to_datetime(prev["date"]).date() < metric.date
            else prev["yesterday_max_profit_on_profitable_trading_days"]
        )
        metric.max_profit_on_profitable_trading_days = (
            max(metric.daily_profit_loss, metric.yesterday_max_profit_on_profitable_trading_days)
            if metric.daily_profit_loss > 0
            else metric.yesterday_max_profit_on_profitable_trading_days
        )
        metric.consistent_score = (
            (1 - (metric.max_profit_on_profitable_trading_days / metric.sum_profit_on_profitable_trading_days)) * 100
            if metric.sum_profit_on_profitable_trading_days > 0
            else 0.0
        )
        metric.total_deposit = prev["total_deposit"] + (
            deal["Profit"]
            if action in [EnDealAction.DEAL_BALANCE]
            and deal["Profit"] > 0
            and "initialize" not in comment
            else 0.0
        )
        metric.total_withdrawal = prev["total_withdrawal"] + (
            deal["Profit"] if action in [EnDealAction.DEAL_BALANCE] and deal["Profit"] < 0 else 0
        )
        metric.count_trades = prev["count_trades"] + (1 if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL] and entry in [EnDealEntry.ENTRY_IN] else 0)
        metric.count_long_trades = prev["count_long_trades"] + (1 if action in [EnDealAction.DEAL_BUY] and entry in [EnDealEntry.ENTRY_IN] else 0)
        metric.count_short_trades = prev["count_short_trades"] + (1 if action in [EnDealAction.DEAL_SELL] and entry in [EnDealEntry.ENTRY_IN] else 0)
        metric.profit_long_trades = prev["profit_long_trades"] + (
            deal["Profit"]
            if action in [EnDealAction.DEAL_BUY]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else 0.0
        )
        metric.profit_short_trades = prev["profit_short_trades"] + (
            deal["Profit"]
            if action in [EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else 0.0
        )
        metric.count_profit_trades = prev["count_profit_trades"] + (
            1
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            and metric.net_profit >= 0
            else 0
        )
        metric.count_loss_trades = prev["count_loss_trades"] + (
            1
            if action in [EnDealAction.DEAL_BUY, EnDealAction.DEAL_SELL]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            and metric.net_profit < 0
            else 0
        )
        metric.gross_profit = prev["gross_profit"] + (
            metric.net_profit
            if action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and metric.net_profit > 0
            else 0.0
        )
        metric.gross_loss = prev["gross_loss"] + (
            metric.net_profit
            if action
            not in [
                EnDealAction.DEAL_BALANCE,
                EnDealAction.DEAL_CREDIT,
                EnDealAction.DEAL_SO_COMPENSATION,
            ]
            and metric.net_profit < 0
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
            metric.net_profit
            if metric.net_profit > prev["best_trade"]
            and action
            in [
                EnDealAction.DEAL_BUY,
                EnDealAction.DEAL_SELL,
            ]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else prev["best_trade"]
        )
        metric.worst_trade = (
            metric.net_profit
            if metric.net_profit < prev["worst_trade"]
            and action
            in [
                EnDealAction.DEAL_BUY,
                EnDealAction.DEAL_SELL,
            ]
            and entry
            in [EnDealEntry.ENTRY_OUT, EnDealEntry.ENTRY_INOUT, EnDealEntry.ENTRY_OUT_BY]
            else prev["worst_trade"]
        )
        metric.average_win = metric.gross_profit / metric.count_profit_trades if metric.count_profit_trades > 0 else 0.0
        metric.average_loss = metric.gross_loss / metric.count_loss_trades if metric.count_loss_trades > 0 else 0.0

        return metric



