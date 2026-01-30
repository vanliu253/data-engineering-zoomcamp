#!/usr/bin/env python
# coding: utf-8

from sqlalchemy import create_engine
import pandas as pd
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db_name', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target_table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')

def ingest_data(user, password, host, port, db_name, target_table, year, month, chunksize):

    iter_df = pd.read_csv(
        prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    first = True

    for df in tqdm(iter_df):
        if first:
            df.head(0).to_sql(name=target_table, con=engine, if_exists='replace') 
            first = False
            print("Table Created")
        else: 
            df.to_sql(name=target_table, con=engine, if_exists='append') 
            print(f"Inserted {len(df)} rows")
        pass



if __name__ == '__main__':
    ingest_data()
