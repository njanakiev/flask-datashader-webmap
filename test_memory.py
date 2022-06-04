import pandas as pd
import geopandas as gpd
import pygeos


@profile
def load_geometry_data(filepath):
    df = pd.read_parquet(filepath, engine='pyarrow', columns=['x', 'y'])
    df = df.rename(columns={'geom': 'geometry'})
    print(df.info(memory_usage='deep'))
    print(df['geometry'].memory_usage(deep=True))

    gdf = gpd.GeoDataFrame(df,
        geometry=gpd.GeoSeries.from_wkb(df['geometry']),
        crs='EPSG:4326')

    #gdf = gpd.GeoDataFrame(df,
    #    geometry=gpd.GeoSeries.from_wkb(df['geometry']),
    #    crs='EPSG:4326')

    print(gdf.info(memory_usage='deep'))
    print(gdf['geometry'].memory_usage(deep=True))

@profile
def load_xy_data(filepath):
    gdf = pd.read_parquet(filepath, engine='pyarrow', columns=['x', 'y'])
    gdf['geometry'] = gpd.points_from_xy(gdf['x'], gdf['y'])
    #gdf['geometry'] = pygeos.points(gdf['x'], gdf['y'])
    gdf = gpd.GeoDataFrame(gdf,
        geometry='geometry',
        crs='EPSG:4326')

    print(gdf.info(memory_usage='deep'))
    print(gdf['geometry'].memory_usage(deep=True))


if __name__ == '__main__':
    filepath = "data/random_1mio_geom.snappy.parquet"
    #filepath = "data/gps_osm_xy.snappy.parq"

    load_xy_data(filepath)
    #load_geometry_data(filepath)
