import os
import time
import shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import dask.dataframe as dd
from dask.distributed import Client

RADIUS_EARTH = 6378137.0


def convert(src_filepath, dst_filepath):
    # Decrease size to minimum
    DTYPES = { "lat": np.float32, "lon": np.float32 }
    schema = pa.schema([
        pa.field('x', pa.float32()),
        pa.field('y', pa.float32()),
    ])
    
    df = dd.read_csv(src_filepath,
                     names=['lat', 'lon'],
                     dtype=DTYPES)
    
    df['lat'] = df['lat'] / 10**7
    df['lon'] = df['lon'] / 10**7

    # Filter points outside
    mask = (-180 < df['lon']) & (df['lon'] < 180) \
         & (-85  < df['lat']) & (df['lat'] < 85)
    df = df[mask]
    
    # Calculate Web Mercator coordinates (epsg:3857)
    df['x'] = RADIUS_EARTH * np.radians(df['lon'])
    df['y'] = RADIUS_EARTH * np.log(
        np.tan((np.pi * 0.25) + (0.5 * np.radians(df['lat']))))

    df[['x', 'y']].to_parquet(
        dst_filepath,
        schema=schema,
        engine='pyarrow',
        compression='snappy')


if __name__ == '__main__':
    client = Client(memory_limit='3GB', processes=True)
    print("Dashboard Link", client.dashboard_link)

    src_filepath = "data/simple-gps-points-120312.txt"
    dst_filepath = "data/gps_osm_xy.snappy.parq"

    if os.path.exists(dst_filepath):
        shutil.rmtree(dst_filepath)
    
    t = time.time()
    convert(src_filepath, dst_filepath)
    print(f"Duration: {time.time() - t:.4f}")
