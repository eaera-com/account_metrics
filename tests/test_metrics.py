import datetime
from unittest.mock import patch
import pandas as pd

from account_metrics.mt5_deal import MT5Deal
from account_metrics.mt5_deal_daily import MT5DealDaily
from account_metrics.account_metric_by_day import AccountMetricDailyCalculator, AccountMetricDaily
from account_metrics.account_metric_by_deal import AccountMetricByDealCalculator, AccountMetricByDeal
from account_metrics.account_symbol_metric_by_deal import AccountSymbolMetricByDealCalculator, AccountSymbolMetricByDeal
from account_metrics.position_metric_by_deal import PositionMetricByDealCalculator, PositionMetricByDeal
from tests.conftest import MockDatastore, MockMetricRunner, extract_type_mapping, setup_string_column_type, strip_quotes_from_string_columns, TEST_DATAFRAME_PATH, get_metric_from_csv

def get_deal(from_timestamp:datetime.date = None,to_timestamp:datetime.date = None):
    return get_metric_from_csv(MT5Deal,TEST_DATAFRAME_PATH[MT5Deal])

def get_history(from_timestamp:datetime.date = None,to_timestamp:datetime.date = None):
    history = get_metric_from_csv(MT5DealDaily, TEST_DATAFRAME_PATH[MT5DealDaily])
    history["Date"] = pd.to_datetime(history["Date"]).dt.date
    if from_timestamp:
        history = history[(history["timestamp_utc"] >= from_timestamp)]
    if to_timestamp:
        history = history[(history["timestamp_utc"] <= to_timestamp)]
    return history 

################################################################## TESTS ##################################################################################
def test_account_metric_by_day_calculation():
    AccountMetricDailyCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, get_history()),
            AccountMetricDaily: MockDatastore(AccountMetricDaily, pd.DataFrame(columns=AccountMetricDaily.model_fields.keys()))
        }
    ))
    deal = get_deal()
    calculated_df = AccountMetricDailyCalculator.calculate(deal)
    calculated_df = setup_string_column_type(calculated_df,AccountMetricDaily)
    
    # Load the expected data from CSV
    expected_df = pd.read_csv(TEST_DATAFRAME_PATH[AccountMetricDaily],dtype=extract_type_mapping(AccountMetricDaily))
    
    # Adopt type and adjust expected different columns
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date
    expected_df.rename(columns={"timestamp":"timestamp_utc"},inplace=True)
    
    # Ensure both dataframes have the same columns
    assert set(calculated_df.columns) == set(expected_df.columns), f"Columns do not match {calculated_df.columns} != {expected_df.columns}"
    # Verify data types of each column
    for column in expected_df.columns:
        assert calculated_df[column].dtype == expected_df[column].dtype, f"Data type mismatch for column {column}: {calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Compare dataframes
    pd.testing.assert_frame_equal(calculated_df[expected_df.columns],expected_df,check_dtype=True)
    
def test_account_metric_by_day_calculation_2():
    AccountMetricDailyCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, pd.DataFrame(columns=MT5DealDaily.model_fields.keys())),
            AccountMetricDaily: MockDatastore(AccountMetricDaily, pd.DataFrame(columns=AccountMetricDaily.model_fields.keys()))
        }
    ))
    deal = get_deal()   # Choose active login
    login = deal["login"].iloc[1]

    ############################ FIRST CALCULATION ############################
    first_retrieve_time = deal["timestamp_utc"].iloc[len(deal)//2-1]
    first_retrieve_deal = deal[(deal["login"] == login) & (deal["timestamp_utc"] < first_retrieve_time)]
    AccountMetricDailyCalculator.get_metric_runner().get_datastore(MT5DealDaily).put(get_history(to_timestamp=first_retrieve_time))
    
    first_calculated_df = AccountMetricDailyCalculator.calculate(first_retrieve_deal)
    first_calculated_df = setup_string_column_type(first_calculated_df,AccountMetricDaily)

    # Load the expected data from CSV
    expected_df = pd.read_csv(TEST_DATAFRAME_PATH[AccountMetricDaily], dtype=extract_type_mapping(AccountMetricDaily))
    expected_df = expected_df[expected_df["login"] == login]
    
    # Adopt type and adjust expected different columns
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date
    expected_df.rename(columns={"timestamp":"timestamp_utc"}, inplace=True)
    
    # Limit the expected data to match the calculated data
    expected_first_df = expected_df.head(len(first_calculated_df))
    
    # Ensure both dataframes have the same columns
    assert set(first_calculated_df.columns) == set(expected_df.columns), f"Columns do not match {first_calculated_df.columns} != {expected_first_df.columns}"
    
    # Verify data types of each column
    for column in expected_df.columns:
        assert first_calculated_df[column].dtype == expected_first_df[column].dtype, f"Data type mismatch for column {column}: {first_calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Compare dataframes
    pd.testing.assert_frame_equal(first_calculated_df[expected_first_df.columns], expected_first_df, check_dtype=True)
    
    ################################################################ SECOND CALCULATION ################################################################

    second_retrieve_data = deal[(deal["login"] == login) & (deal["timestamp_utc"] >= first_retrieve_time)]
    AccountMetricDailyCalculator.get_metric_runner().get_datastore(MT5DealDaily).put(get_history(from_timestamp=first_retrieve_time))
    AccountMetricDailyCalculator.get_metric_runner().get_datastore(AccountMetricDaily).put(first_calculated_df[expected_first_df.columns])
        
    second_calculated_df = AccountMetricDailyCalculator.calculate(second_retrieve_data)
    second_calculated_df = setup_string_column_type(second_calculated_df,AccountMetricDaily)
    
    # Get the rest of expected_df
    expected_second_df = expected_df.iloc[len(first_calculated_df):]
    expected_second_df = expected_second_df.reset_index(drop=True)

    pd.testing.assert_frame_equal(second_calculated_df[expected_second_df.columns], expected_second_df, check_dtype=True)
    
def test_account_metric_by_deal_calculation():
    AccountMetricByDealCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, get_history()),
             AccountMetricByDeal:  MockDatastore(AccountMetricByDeal, pd.DataFrame(columns=AccountMetricByDeal.model_fields.keys()))
        }
    ))
    deal = get_deal()
    calculated_df = AccountMetricByDealCalculator.calculate(deal)
    calculated_df = setup_string_column_type(calculated_df,AccountMetricByDeal)
        
    # Load the expected data from CSV
    expected_df = pd.read_csv(TEST_DATAFRAME_PATH[AccountMetricByDeal], dtype=extract_type_mapping(AccountMetricByDeal))
    
    # Adopt type and adjust expected different columns
    expected_df = strip_quotes_from_string_columns(expected_df)
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date
    expected_df.rename(columns={"timestamp":"timestamp_utc"},inplace=True)

    # Ensure both dataframes have the same columns
    assert set(calculated_df.columns) == set(expected_df.columns), f"Columns do not match {calculated_df.columns} != {expected_df.columns}"
    # Verify data types of each column
    for column in expected_df.columns:
        assert calculated_df[column].dtype == expected_df[column].dtype, f"Data type mismatch for column {column}: {calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Compare dataframes
    pd.testing.assert_frame_equal(calculated_df[expected_df.columns],expected_df,check_dtype=True)
    
def test_account_metric_by_deal_calculation_2():
    AccountMetricByDealCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, get_history()),
             AccountMetricByDeal:  MockDatastore(AccountMetricByDeal, pd.DataFrame(columns=AccountMetricByDeal.model_fields.keys()))
        }
    ))
            
    deal = get_deal()
    login = deal["login"].iloc[1]
    ############################ FIRST CALCULATION ############################
    first_retrieve_time = deal["timestamp_utc"].iloc[len(deal)//2-1]
    first_retrieve_deal = deal[(deal["login"] == login) & (deal["timestamp_utc"] < first_retrieve_time)]
    AccountMetricByDealCalculator.get_metric_runner().get_datastore(MT5DealDaily).put(get_history(to_timestamp=first_retrieve_time))
    
    first_calculated_df = AccountMetricByDealCalculator.calculate(first_retrieve_deal)
    first_calculated_df = setup_string_column_type(first_calculated_df,AccountMetricByDeal)

    # Load the expected data from CSV
    expected_df = pd.read_csv(TEST_DATAFRAME_PATH[AccountMetricByDeal], dtype=extract_type_mapping(AccountMetricByDeal))
    expected_df = expected_df[expected_df["login"] == login]
    
    # Adopt type and adjust expected different columns
    expected_df = strip_quotes_from_string_columns(expected_df)
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date
    expected_df.rename(columns={"timestamp":"timestamp_utc"},inplace=True)
    
    # Limit the expected data to match the calculated data
    expected_first_df = expected_df.head(len(first_calculated_df))
    
    # Ensure both dataframes have the same columns
    assert set(first_calculated_df.columns) == set(expected_df.columns), f"Columns do not match {first_calculated_df.columns} != {expected_first_df.columns}"
    
    # Verify data types of each column
    for column in expected_df.columns:
        assert first_calculated_df[column].dtype == expected_first_df[column].dtype, f"Data type mismatch for column {column}: {first_calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Compare dataframes
    pd.testing.assert_frame_equal(first_calculated_df[expected_first_df.columns], expected_first_df, check_dtype=True)
    
    ################################################################ SECOND CALCULATION ################################################################
    second_retrieve_data = deal[(deal["login"] == login) & (deal["timestamp_utc"] >= first_retrieve_time)]
    AccountMetricByDealCalculator.get_metric_runner().get_datastore(MT5DealDaily).put(get_history(from_timestamp=first_retrieve_time))
    AccountMetricByDealCalculator.get_metric_runner().get_datastore(AccountMetricByDeal).put(first_calculated_df[expected_first_df.columns])
            
    second_calculated_df = AccountMetricByDealCalculator.calculate(second_retrieve_data)
    second_calculated_df = setup_string_column_type(second_calculated_df,AccountMetricByDeal)
        
    # Get the rest of expected_df
    expected_second_df = expected_df.iloc[len(first_calculated_df):]
    expected_second_df = expected_second_df.reset_index(drop=True)

    pd.testing.assert_frame_equal(second_calculated_df[expected_second_df.columns], expected_second_df, check_dtype=True)

def test_account_symbol_metric_by_deal_calculation():
    AccountSymbolMetricByDealCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, get_history()),
             AccountSymbolMetricByDeal:  MockDatastore(AccountSymbolMetricByDeal, pd.DataFrame(columns=AccountSymbolMetricByDeal.model_fields.keys()))
        }
    ))
    
    deal = get_deal()
    calculated_df = AccountSymbolMetricByDealCalculator.calculate(deal)
    calculated_df = setup_string_column_type(calculated_df,AccountSymbolMetricByDeal)

    # Load the expected data from CSV
    expected_df = pd.read_csv('tests/test_data/account_symbol_metric_by_deal.csv', dtype=extract_type_mapping(AccountSymbolMetricByDeal))

    # Adopt type and adjust expected different columns
    expected_df = strip_quotes_from_string_columns(expected_df)
    expected_df.rename(columns={"timestamp":"timestamp_utc"},inplace=True)

    assert set(calculated_df.columns) == set(expected_df.columns), f"Columns do not match {calculated_df.columns} != {expected_df.columns}"

    # Verify data types of each column
    for column in expected_df.columns:
        assert calculated_df[column].dtype == expected_df[column].dtype, f"Data type mismatch for column {column}: {calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Compare dataframes
    pd.testing.assert_frame_equal(calculated_df[expected_df.columns],expected_df,check_dtype=True)
    
def test_account_symbol_metric_by_deal_calculation_2():
    AccountSymbolMetricByDealCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, get_history()),
             AccountSymbolMetricByDeal:  MockDatastore(AccountSymbolMetricByDeal, pd.DataFrame(columns=AccountSymbolMetricByDeal.model_fields.keys()))
        }
    ))
    
    deal = get_deal()
    login = deal["login"].iloc[1]# Choose active login
    ################################################################ FIRST CALCULATION ################################################################
    first_retrieve_time = deal["timestamp_utc"].iloc[len(deal)//2-1]
    first_retrieve_deal = deal[(deal["login"] == login) & (deal["timestamp_utc"] < first_retrieve_time)]
    AccountSymbolMetricByDealCalculator.get_metric_runner().get_datastore(MT5DealDaily).put(get_history(to_timestamp=first_retrieve_time))
    
    first_calculated_df = AccountSymbolMetricByDealCalculator.calculate(first_retrieve_deal)
    first_calculated_df = setup_string_column_type(first_calculated_df, AccountSymbolMetricByDeal)
    
    # Load the expected data from CSV
    expected_df = pd.read_csv('tests/test_data/account_symbol_metric_by_deal.csv', dtype=extract_type_mapping(AccountSymbolMetricByDeal))
    expected_df = expected_df[expected_df["login"] == login]

    # Adopt type and adjust expected different columns
    expected_df = strip_quotes_from_string_columns(expected_df)
    expected_df.rename(columns={"timestamp":"timestamp_utc"}, inplace=True)
    
    # Limit the expected data to match the calculated data
    expected_first_df = expected_df.head(len(first_calculated_df))
    
    # Ensure both dataframes have the same columns
    assert set(first_calculated_df.columns) == set(expected_df.columns), f"Columns do not match {first_calculated_df.columns} != {expected_first_df.columns}"
    
    # Verify data types of each column
    for column in expected_df.columns:
        assert first_calculated_df[column].dtype == expected_first_df[column].dtype, f"Data type mismatch for column {column}: {first_calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Compare dataframes
    pd.testing.assert_frame_equal(first_calculated_df[expected_first_df.columns], expected_first_df, check_dtype=True)
    
    ################################################################ SECOND CALCULATION ################################################################
    second_retrieve_data = deal[(deal["login"] == login) & (deal["timestamp_utc"] >= first_retrieve_time)]
    AccountSymbolMetricByDealCalculator.get_metric_runner().get_datastore(MT5DealDaily).put(get_history(from_timestamp=first_retrieve_time))
    AccountSymbolMetricByDealCalculator.get_metric_runner().get_datastore(AccountSymbolMetricByDeal).put(first_calculated_df[expected_first_df.columns])
        

    second_calculated_df = AccountSymbolMetricByDealCalculator.calculate(second_retrieve_data)
    second_calculated_df = setup_string_column_type(second_calculated_df, AccountSymbolMetricByDeal)

    # Get the rest of expected_df
    expected_second_df = expected_df.iloc[len(first_calculated_df):]
    expected_second_df = expected_second_df.reset_index(drop=True)

    pd.testing.assert_frame_equal(second_calculated_df[expected_second_df.columns], expected_second_df, check_dtype=True)

def test_position_metric_by_deal_calculation():
    PositionMetricByDealCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, get_history()),
             PositionMetricByDeal:  MockDatastore(PositionMetricByDeal, pd.DataFrame(columns=PositionMetricByDeal.model_fields.keys()))
        }
    ))
    
    deal = get_deal()
    calculated_df = PositionMetricByDealCalculator.calculate(deal)
    calculated_df = setup_string_column_type(calculated_df,PositionMetricByDeal)
    
    # Load the expected data from CSV
    expected_df = pd.read_csv(TEST_DATAFRAME_PATH[PositionMetricByDeal], dtype=extract_type_mapping(PositionMetricByDeal))
   
    # Adopt type and adjust expected different columns
    expected_df = strip_quotes_from_string_columns(expected_df)
    expected_df.rename(columns={"timestamp":"timestamp_utc"},inplace=True)
    
    # Ensure both dataframes have the same columns
    assert set(calculated_df.columns) == set(expected_df.columns), f"Columns do not match {calculated_df.columns} != {expected_df.columns}"

    # Verify data types of each column
    for column in expected_df.columns:
        assert calculated_df[column].dtype == expected_df[column].dtype, f"Data type mismatch for column {column}: {calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Rearragne columns order and compare dataframes
    pd.testing.assert_frame_equal(calculated_df[expected_df.columns],expected_df,check_dtype=True)

def test_position_metric_by_deal_calculation_2():
    PositionMetricByDealCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, get_history()),
             PositionMetricByDeal:  MockDatastore(PositionMetricByDeal, pd.DataFrame(columns=PositionMetricByDeal.model_fields.keys()))
        }
    ))
    
    deal = get_deal()
    login = deal["login"].iloc[1]

    ################################################################ FIRST CALCULATION ################################################################
    first_retrieve_time = deal["timestamp_utc"].iloc[len(deal)//2-1]
    first_retrieve_deal =  deal[(deal["timestamp_utc"] < first_retrieve_time)]
    PositionMetricByDealCalculator.get_metric_runner().get_datastore(MT5DealDaily).put(get_history(to_timestamp=first_retrieve_time))
    
    first_calculated_df = PositionMetricByDealCalculator.calculate(first_retrieve_deal)
    first_calculated_df = setup_string_column_type(first_calculated_df, PositionMetricByDeal)
    
    # Load the expected data from CSV
    expected_df = pd.read_csv(TEST_DATAFRAME_PATH[PositionMetricByDeal], dtype=extract_type_mapping(PositionMetricByDeal))
    
    # Adopt type and adjust expected different columns
    expected_df = strip_quotes_from_string_columns(expected_df)
    expected_df.rename(columns={"timestamp":"timestamp_utc"}, inplace=True)
    
    # Sort the dataframes by deal_id and position_id
    expected_df.sort_values(by=["deal_id", "position_id"], inplace=True)
    expected_df.reset_index(drop=True, inplace=True)
    first_calculated_df.sort_values(by=["deal_id", "position_id"], inplace=True)
    first_calculated_df.reset_index(drop=True, inplace=True)
    
    # Limit the expected data to match the calculated data
    expected_first_df = expected_df.head(len(first_calculated_df))
    
    # Ensure both dataframes have the same columns
    assert set(first_calculated_df.columns) == set(expected_df.columns), f"Columns do not match {first_calculated_df.columns} != {expected_first_df.columns}"
    
    # Verify data types of each column
    for column in expected_df.columns:
        assert first_calculated_df[column].dtype == expected_first_df[column].dtype, f"Data type mismatch for column {column}: {first_calculated_df[column].dtype} != {expected_df[column].dtype}"

    # Compare dataframes
    pd.testing.assert_frame_equal(first_calculated_df[expected_first_df.columns], expected_first_df, check_dtype=True)
    ################################################################ SECOND CALCULATION ################################################################
    second_retrieve_deal = deal[(deal["login"] == login) & (deal["timestamp_utc"] >= first_retrieve_time)]
    PositionMetricByDealCalculator.get_metric_runner().get_datastore(MT5DealDaily).put(get_history(from_timestamp=first_retrieve_time))
    PositionMetricByDealCalculator.get_metric_runner().get_datastore(PositionMetricByDeal).put(first_calculated_df[expected_first_df.columns])
    
    second_calculated_df = PositionMetricByDealCalculator.calculate(second_retrieve_deal)
    second_calculated_df = setup_string_column_type(second_calculated_df, PositionMetricByDeal)
    
    # Get the rest of expected_df
    expected_second_df = expected_df.iloc[len(first_calculated_df):]
    expected_second_df = expected_second_df.reset_index(drop=True)
    
    # Sort the dataframes by deal_id and position_id
    second_calculated_df.sort_values(by=["deal_id", "position_id"], inplace=True)
    second_calculated_df.reset_index(drop=True, inplace=True)

    pd.testing.assert_frame_equal(second_calculated_df[expected_second_df.columns], expected_second_df, check_dtype=True)


def get_perf_test_df():
    csv_path = "tests/test_data/large/auda_deals.csv"
    df = get_metric_from_csv(MT5Deal, csv_path)
    return df

def test_perf():
    start_time = time.time()
    
    AccountMetricByDealCalculator.set_metric_runner(MockMetricRunner(
        {
            MT5DealDaily: MockDatastore(MT5DealDaily, get_history()),
             AccountMetricByDeal:  MockDatastore(AccountMetricByDeal, pd.DataFrame(columns=AccountMetricByDeal.model_fields.keys()))
        }
    ))
    deals = get_perf_test_df()
    
    AccountMetricByDealCalculator.calculate(deals)
    end_time = time.time()  

    elapsed_time = end_time - start_time 
    assert elapsed_time > 10**15    # just so that it fails
    