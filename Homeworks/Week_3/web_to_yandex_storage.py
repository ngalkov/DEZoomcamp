import os 
from typing import Optional
from pathlib import Path

import requests
import boto3
from dotenv import load_dotenv


dotenv_file = Path('.env')
if dotenv_file.is_file():
    load_dotenv(dotenv_file)

S3_ENDPOINT_URL =  os.environ.get('S3_ENDPOINT_URL')
REGION_NAME = os.environ.get('REGION_NAME')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY =  os.environ.get('AWS_SECRET_ACCESS_KEY')

BUCKET_NAME = 'prefect-de-zoomcamp'


def download_file(url: str, local_path: Path) -> None:
    with open(local_path, "wb") as out_file:
        response = requests.get(url)
        out_file.write(response.content)


def upload_file_to_s3(local_path: Path, bucket_name: str, object_name: Optional[str] = None) -> None:
    if object_name is None:
        object_name = local_path.name

    session = boto3.session.Session(
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        region_name = REGION_NAME
    )
    s3 = session.client(
        service_name='s3',
        endpoint_url = S3_ENDPOINT_URL, 
    )
    s3.upload_file(local_path, bucket_name, object_name)

def upload_nytaxi_to_yandex_storage(trip_type: str, year: int, months: list[int], ):
    for month in months:
        file_name = f"{trip_type}_tripdata_{year}-{month:02}.csv.gz"
        base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
        url = f'{base_url}/{trip_type}/{file_name}'
        local_path = Path(f'./data/{trip_type}/{file_name}')
        print(f'Loading {url} into {local_path} ...')
        download_file(url, local_path)
        object_name = f'data/{trip_type}/{file_name}'
        print(f'Uploading {local_path} into Yandex object storage as {object_name} ...')
        upload_file_to_s3(local_path, BUCKET_NAME, object_name)
        

if __name__ == '__main__':
    upload_nytaxi_to_yandex_storage('fhv', 2019, range(1, 13))
