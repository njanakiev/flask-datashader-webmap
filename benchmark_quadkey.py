import os
import time
import numpy as np
import pandas as pd
import mercantile
import datashader as ds
from datashader import transfer_functions as tf
import dask.dataframe as dd
from dask.distributed import Client
import quadkey
from dotenv import load_dotenv

load_dotenv()

S3_STORAGE_OPTIONS = {
    'key': os.environ['S3_ACCESS_KEY'],
    'secret': os.environ['S3_SECRET_KEY'],
    "client_kwargs": {
        "endpoint_url": os.environ['S3_ENDPOINT'],
    },
    "config_kwargs": {"s3": {"addressing_style": "virtual"}},
}


def create_datashade_image_quadkey(src_filepath, x, y, zoom, 
                                   storage_options=None):
    qk = quadkey.xyz2quadint(x, y, zoom)
    xy_bounds = quadkey.tile2bbox_webmercator(qk, zoom)
    qk1, qk2 = quadkey.tile2range(qk, zoom)

    filters = [("quadkey", ">", np.uint64(qk1)),
               ("quadkey", "<", np.uint64(qk2))]

    df = dd.read_parquet(
        src_filepath, 
        filters=filters, 
        engine='pyarrow',
        columns=['x', 'y'],
        storage_options=storage_options)

    mask = (qk1 < df.index) & (df.index < qk2)

    cvs = ds.Canvas(plot_width=256, plot_height=256,
        x_range=(xy_bounds[0], xy_bounds[2]), 
        y_range=(xy_bounds[1], xy_bounds[3]))
    agg = cvs.points(df.loc[mask], 'x', 'y', agg=ds.count())
    img = tf.shade(agg,
        cmap=['blue', 'darkblue', 'black'],
        how='eq_hist')
    img = tf.spread(img, name="spread 1px")

    return img.to_pil()


def count_quadkey_partitions(src_filepath, x, y, zoom, 
                             storage_options=None):
    qk = quadkey.xyz2quadint(x, y, zoom)
    xy_bounds = quadkey.tile2bbox_webmercator(qk, zoom)
    qk1, qk2 = quadkey.tile2range(qk, zoom)

    filters = [("quadkey", ">", np.uint64(qk1)),
               ("quadkey", "<", np.uint64(qk2))]

    df = dd.read_parquet(
        src_filepath, 
        filters=filters, 
        engine='pyarrow',
        columns=['x', 'y'], 
        storage_options=storage_options)

    return df.npartitions


def benchmark_tiles(src_filepath, dst_filepath,
                    min_zoom=0, max_zoom=5,
                    storage_options=None):
    items = []
    for zoom in range(min_zoom, max_zoom + 1):
        #steps = 2**zoom
        steps = 1
        for x in range(steps):
            for y in range(steps):
                t = time.time()
                img = create_datashade_image_quadkey(
                    src_filepath, x, y, zoom, storage_options)
                duration = time.time() - t
                items.append({
                    'x': x,
                    'y': y,
                    'zoom': zoom,
                    'duration': duration
                })
                print(f"({x},{y},{zoom}), " \
                      f"duration: {duration:.4f}")

    pd.DataFrame(items).to_csv(
        dst_filepath, index=False)


def benchmark_tiles_partitions(src_filepath, dst_filepath,
                               min_zoom=0, max_zoom=5,
                               storage_options=None):
    items = []
    for zoom in range(min_zoom, max_zoom + 1):
        steps = 2**zoom
        #steps = 1
        for x in range(steps):
            for y in range(steps):
                t = time.time()
                npartitions = count_quadkey_partitions(
                    src_filepath, x, y, zoom, storage_options)
                duration = time.time() - t
                items.append({
                    'x': x,
                    'y': y,
                    'zoom': zoom,
                    'duration': duration,
                    'npartitions': npartitions
                })
                print(f"({x},{y},{zoom}), " \
                      f"duration: {duration:.4f}, "\
                      f"npartitions {npartitions}")

    pd.DataFrame(items).to_csv(
        dst_filepath, index=False)


if __name__ == '__main__':
    client = Client(memory_limit='3GB', processes=True, n_workers=2)
    print("Dashboard Link", client.dashboard_link)

    #src_filepath = "data/gps_points_10mio.snappy.parq"
    src_filepath = 's3://gdelt/osm_gps_parquet/osm_gps.snappy.parq'
    #src_filepath = "osm_gps.snappy.parq"
    #image_filepath = "frame.png"

    #df = dd.read_parquet(src_filepath, engine='pyarrow')
    #print(df.head())

    #img = create_datashade_image(df)
    #img.save(image_filepath)

    #dst_filepath = "data/tiles_partitions_benchmark_5.csv"
    dst_filepath = "data/tiles_benchmark_3_4.csv"
    benchmark_tiles(
        src_filepath, dst_filepath, 
        min_zoom=3, max_zoom=4, 
        storage_options=S3_STORAGE_OPTIONS)
