import os
import time
import pandas as pd
import mercantile
import datashader as ds
from datashader import transfer_functions as tf

import dask.dataframe as dd
from dask.distributed import Client
from distributed import LocalCluster, Client

import spatialpandas as spd
from spatialpandas import GeoSeries, GeoDataFrame
from spatialpandas.geometry import PointArray, PolygonArray
from spatialpandas.io import read_parquet, read_parquet_dask

import shapely
import geopandas as gpd


def create_datashade_image(df, x, y, z):
    xy_bounds = list(mercantile.xy_bounds(
        mercantile.Tile(x, y, z)))
    
    #bbox = shapely.geometry.box(*xy_bounds)
    #bbox_gdf = GeoDataFrame(gpd.GeoSeries({'geometry': bbox}))

    bbox_gdf = GeoDataFrame(PolygonArray([[xy_bounds]]))
    rdf = spd.sjoin(df, bbox_gdf, how='inner').compute()

    cvs = ds.Canvas(plot_width=256, plot_height=256,
        x_range=(xy_bounds[0], xy_bounds[2]), 
        y_range=(xy_bounds[1], xy_bounds[3]))
    agg = cvs.points(rdf, 'x', 'y', agg=ds.count())
    img = tf.shade(agg,
        cmap=['blue', 'darkblue', 'black'],
        how='eq_hist')
    img = tf.spread(img, name="spread 1px")

    return img


def benchmark_tiles(src_filepath, min_zoom=0, max_zoom=5):
    df = read_parquet_dask(src_filepath)
    
    items = []
    for zoom in range(min_zoom, max_zoom + 1):
        steps = 2**zoom
        #steps = 1
        for x in range(steps):
            for y in range(steps):
                t = time.time()
                img = create_datashade_image(df, x, y, zoom)
                duration = time.time() - t
                items.append({
                    'x': x,
                    'y': y,
                    'zoom': zoom,
                    'duration': duration
                })
                print(f"({x},{y},{zoom}), duration: {duration:.4f}")

    return items


if __name__ == '__main__':
    client = Client(memory_limit='3GB', processes=True)
    print("Dashboard Link", client.dashboard_link)

    src_filepath = "data/gps_points_100mio_spatialpandas.snappy.parq"

    items = benchmark_tiles(src_filepath, max_zoom=3)
    pd.DataFrame(items).to_csv("tiles_benchmark.csv", index=False)
