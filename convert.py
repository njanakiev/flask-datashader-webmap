import os
import time
import shutil
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import quadkey
import pyarrow as pa

# 2,770,233,904 lines for simple-gps-points-120312.txt
# 2,899,550,650 lines for simple-gps-points-120604.csv

RADIUS_EARTH = 6378137.0


def calculate_quadkeys(df):
    df['quadkey'] = df.apply(
        lambda row: quadkey.lonlat2quadint(row['lon'], row['lat']),
        axis=1)
    return df[['x', 'y', 'quadkey']]


def preprocess(src_filepath, dst_filepath):
    DTYPES = { "lat": np.float64, "lon": np.float64 }
    
    df = dd.read_csv(src_filepath,
                     #skiprows=1,
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

    df.to_parquet(dst_filepath,
                  engine='pyarrow',
                  compression='snappy')

    #df.set_index("quadkey", shuffle="disk") \
    #    .to_parquet(dst_filepath,
    #                engine='pyarrow',
    #                compression='snappy')


# real    471m16.067s
# user    1815m39.629s
# sys     49m32.892s
def compute_quadkeys(src_filepath, dst_filepath):
    df = dd.read_parquet(src_filepath,
                         engine='pyarrow')

    #df['quadkey'] = df.apply(
    #    lambda row: quadkey.lonlat2quadint(row['lon'], row['lat']),
    #    meta=pd.Series(dtype=np.uint64, name='quadkey'),
    #    axis=1)

    df = df.map_partitions(
        calculate_quadkeys,
        meta={
            #'lat': np.float64,
            #'lon': np.float64,
            'x': np.float32,
            'y': np.float32,
            'quadkey': np.uint64
        })

    schema = pa.schema([
        pa.field('x', pa.float32()),
        pa.field('y', pa.float32()),
        pa.field('quadkey', pa.uint64()),
    ])

    df.to_parquet(dst_filepath,
                  schema=schema,
                  engine='pyarrow',
                  compression='snappy')


# real    247m57.891s          
# user    79m46.654s           
# sys     39m2.646s
def sort_quadkeys(src_filepath, dst_filepath):
    df = dd.read_parquet(src_filepath,
                         engine='pyarrow')

    df.set_index("quadkey", shuffle="disk") \
        .to_parquet(dst_filepath,
                    engine='pyarrow',
                    compression='snappy')


if __name__ == '__main__':
    client = Client(memory_limit='6GB', processes=True)
    print("Dashboard Link", client.dashboard_link)

    #src_filepath = "data/simple-gps-points-120604.csv"
    #src_filepath = "data/simple-gps-points-120312.txt"
    src_filepath = "data/gps_points_10mio.txt"
    tmp_filepath = "data/tmp.snappy.parq"
    dst_filepath = "data/gps_points_10mio_quadkey.snappy.parq"

    #if os.path.exists(tmp_filepath):
    #    shutil.rmtree(tmp_filepath)
    if os.path.exists(dst_filepath):
        shutil.rmtree(dst_filepath)
    
    print("CONVERT")
    t = time.time()
    preprocess(src_filepath, dst_filepath)
    print(f"Duration: {time.time() - t:.4f}")

    #print("PREPROCESS")
    #t = time.time()
    #sort_quadkeys(tmp_filepath, dst_filepath)
    #print(f"Duration: {time.time() - t:.4f}")
