import os
import shutil
import dask.dataframe as dd
from distributed import LocalCluster, Client
import spatialpandas as spd
from spatialpandas import GeoSeries, GeoDataFrame
from spatialpandas.geometry import PointArray
from dask.distributed import Client


# 100 mio
# real    6m43.754s
# user    7m36.316s
# sys     0m49.011s
if __name__ == '__main__':
    client = Client(memory_limit='3GB', processes=True)
    print("Dashboard Link", client.dashboard_link)
    
    src_filepath = "data/gps_points_10mio.snappy.parq"
    dst_filepath = "data/gps_points_10mio_spatialpandas.parq"

    if os.path.exists(dst_filepath):
        shutil.rmtree(dst_filepath)

    ddf = dd.read_parquet(
        src_filepath, 
        engine='pyarrow')  
    
    df = ddf.map_partitions(
        lambda df: GeoDataFrame(dict(
            position=PointArray(df[['x', 'y']]),
            **{col: df[col] for col in df.columns}
        ))
    )

    # spatially sort the data
    df.pack_partitions(
        npartitions=df.npartitions, 
        shuffle='disk').to_parquet(dst_filepath)
