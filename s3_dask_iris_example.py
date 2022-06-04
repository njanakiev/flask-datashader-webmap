import os
import time
import pandas as pd
import mercantile
import datashader as ds
from datashader import transfer_functions as tf
import dask.dataframe as dd
from dask.distributed import Client
import quadkey
from dotenv import load_dotenv

load_dotenv()


if __name__ == '__main__':
    client = Client(memory_limit='6GB', processes=True)
    print("Dashboard Link", client.dashboard_link)

    storage_options = {
        'key': os.environ['S3_ACCESS_KEY'],
        'secret': os.environ['S3_SECRET_KEY'],
        "client_kwargs": {
            "endpoint_url": os.environ['S3_ENDPOINT'],
        },
        "config_kwargs": {"s3": {"addressing_style": "virtual"}},
    }

    src_filepath = 's3://gdelt/iris_parquet/iris.parq'

    df = dd.read_parquet(
        src_filepath, 
        storage_options=storage_options,
        engine='pyarrow')
    
    print(df.head())
