import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    urls = ['https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet', 
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet',
            'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet']
    
    # Define columns to parse as dates
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    
    dfs = []
    for url in urls:
    # Load data for each month from the URL
        df = pd.read_parquet(url)
        for column in parse_dates:
            if column in df.columns:
                df[column] = pd.to_datetime(df[column])

        dfs.append(df)
    # Concatenate all DataFrames into a single DataFrame
    final_data = pd.concat(dfs, ignore_index=True)
    return final_data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
