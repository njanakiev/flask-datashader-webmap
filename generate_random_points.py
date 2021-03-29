import os
import shutil
import numpy as np
import pandas as pd
import pyarrow as pa
import geopandas as gpd
import pygeos

RADIUS_EARTH = 6378137.0


def generate_coordinates(dist_type='random', n=10**7):
    if dist_type == 'random': 
        X = np.random.random((n, 2)).astype(np.float32)
        X[:, 0] = (360 * X[:, 0]) - 180
        X[:, 1] = (170 * X[:, 1]) - 85
        return pd.DataFrame(X, columns=['lon', 'lat'])
    elif dist_type == 'grid':
        x, y = np.meshgrid(
            np.linspace(-180, 180, n), 
            np.linspace(-85,  85,  n))
        x, y = x.flatten(), y.flatten()
        X = np.column_stack((x, y)).astype(np.float32)
        return pd.DataFrame(X, columns=['lon', 'lat']) 
    else:
        raise NotImplementedError


def create_parquet(df, dst_filepath):
    df['x'] = RADIUS_EARTH * np.radians(df['lon'])
    df['y'] = RADIUS_EARTH * np.log(
        np.tan((np.pi * 0.25) + (0.5 * np.radians(df['lat']))))

    df['geometry'] = pygeos.io.to_wkb(
        pygeos.creation.points(df['lon'], df['lat']))

    df = df.drop(columns=['lon', 'lat'])
    df.to_parquet(dst_filepath, 
        engine='pyarrow', 
        compression='snappy')


def create_geoparquet(df, dst_filepath):
    gdf = gpd.GeoDataFrame(df, 
        geometry=gpd.points_from_xy(df.lon, df.lat), 
        crs="EPSG:4326")

    gdf['x'] = RADIUS_EARTH * np.radians(gdf['lon'])
    gdf['y'] = RADIUS_EARTH * np.log(
        np.tan((np.pi * 0.25) + (0.5 * np.radians(gdf['lat']))))

    gdf = gdf.drop(columns=['lon', 'lat'])
    gdf.to_parquet(dst_filepath, compression='snappy')


if __name__ == '__main__':
    dst_filepath = "data/random_1mio_geom.snappy.parquet"
    #dst_filepath = "data/random_1mio.txt"
    
    if os.path.exists(dst_filepath):
        if os.path.isdir(dst_filepath):
            shutil.rmtree(dst_filepath)
        else:
            os.remove(dst_filepath)
    
    df = generate_coordinates(n=10**6)
    
    #df.to_csv(dst_filepath, index=False)
    #create_parquet(df, dst_filepath)
    create_geoparquet(df, dst_filepath)
