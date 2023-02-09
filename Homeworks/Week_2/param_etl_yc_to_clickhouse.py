from pathlib import Path
from datetime import timedelta
import pandas as pd
from prefect import flow, task
from prefect.blocks.system import Secret, String
from prefect_aws.s3 import S3Bucket 
from prefect.tasks import task_input_hash
import clickhouse_connect


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_yc(color: str, year: int, month: int) -> Path:
    """Download trip data from Yandex Cloud"""
    yc_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    local_path = yc_path
    yc_block = S3Bucket.load("zoom-yc")
    yc_block.download_object_to_path(from_path=yc_path, to_path=local_path)
    return Path(local_path)


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_clickhouse(df: pd.DataFrame) -> None:
    """Write DataFrame to ClickHouse"""
    # host = String.load("clickhouse-host")
    client = clickhouse_connect.get_client(
        host=String.load("clickhouse-host").value, 
        port=8443, 
        user=Secret.load("clickhouse-user").get(), 
        password=Secret.load("clickhouse-password").get(),
        secure=True,
        verify=True,
        ca_cert='CA.pem',
        database='dezoomcamp'
        )

    create_rides_query = \
    '''CREATE TABLE IF NOT EXISTS "rides" (
        "VendorID" Float64 NULL,
        "tpep_pickup_datetime" DateTime,
        "tpep_dropoff_datetime" DateTime,
        "passenger_count" Int64 NULL,
        "trip_distance" Float64 NULL,
        "RatecodeID" Int64 NULL,
        "store_and_fwd_flag" String NULL,
        "PULocationID" Int64 NULL,
        "DOLocationID" Int64 NULL,
        "payment_type" Float64 NULL,
        "fare_amount" Float64 NULL,
        "extra" Float64 NULL,
        "mta_tax" Float64 NULL,
        "tip_amount" Float64 NULL,
        "tolls_amount" Float64 NULL,
        "improvement_surcharge" Float64 NULL,
        "total_amount" Float64 NULL,
        "congestion_surcharge" Float64 NULL
    )
    ENGINE = MergeTree
    ORDER BY tpep_pickup_datetime'''

    client.command(create_rides_query)
    client.insert_df(table='rides', df=df)


@flow(log_prints=True)
def etl_yc_to_clickhouse(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"):
    """Main ETL flow to load data into Big Query"""
    total_rows = 0
    for month in months:
        path = extract_from_yc(color, year, month)
        
        # df = transform(path)
        df = pd.read_parquet(path)

        total_rows += len(df)
        write_clickhouse(df)
    print(f"total_rows: {total_rows}")


if __name__ == "__main__":
    etl_yc_to_clickhouse()
