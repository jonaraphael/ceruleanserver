#%%
from db_connection import session_scope
from utils.s3 import sync_grds_and_vecs
from data_objects import Slick_Ext, Grd_Ext, Inference_Ext, Posi_Poly_Ext
from sqlalchemy import func, desc
from shapely.geometry import MultiPolygon
from pathlib import Path
from configs import path_config
import geopandas
import pandas as pd
import rasterio
import matplotlib.pyplot as plt
import rasterio.plot
from osgeo import gdal
from datetime import datetime, date
import csv
from geoalchemy2.shape import to_shape
import shutil

db = "cerulean"
processed_pids = []
outpath = Path(path_config.LOCAL_DIR) / "temp" / "outputs"
class_int_dict = {
    "n": 0,
    "v": 1,
    "i": 2,
    "a": 3,
}


def get_unclassified_polys(sess):
    q = (
        sess.query(Grd_Ext)
        .join(Inference_Ext)
        .join(Posi_Poly_Ext)
        .join(Slick_Ext)
        .filter(Slick_Ext.class_int == None)
        .order_by(
            desc(
                1
                / func.ST_Area(Posi_Poly_Ext.geometry)
                * func.ST_Perimeter(Posi_Poly_Ext.geometry)
                * func.ST_Perimeter(Posi_Poly_Ext.geometry)
            )
        )
        .distinct()
    )
    return q.all()


def download_grds(grds, separate_process):
    sync_grds_and_vecs([grd.pid for grd in grds], separate_process)


from IPython.display import clear_output
def plot4(rast, vect):
    clear_output(wait=True)
    width = abs(vect.total_bounds[2] - vect.total_bounds[0])
    height = abs(vect.total_bounds[3] - vect.total_bounds[1])

    s = datetime.now()
    fig, ax = plt.subplots(figsize=(10, 10))
    rasterio.plot.show(rast, ax=ax)
    vect.plot(facecolor="none", edgecolor="red", ax=ax)
    plt.show()
    print(datetime.now() - s)

    fig, ax = plt.subplots(figsize=(10, 10))
    rasterio.plot.show(rast, ax=ax)
    vect.plot(facecolor="none", edgecolor="none", ax=ax)
    plt.show()
    print(datetime.now() - s)

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.set_xlim(([vect.total_bounds[0] - width / 4, vect.total_bounds[2] + width / 4]))
    ax.set_ylim(
        ([vect.total_bounds[1] - height / 4, vect.total_bounds[3] + height / 4])
    )
    rasterio.plot.show(rast, ax=ax)
    vect.plot(facecolor="none", edgecolor="red", ax=ax)
    plt.show()
    print(datetime.now() - s)

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.set_xlim(([vect.total_bounds[0] - width / 4, vect.total_bounds[2] + width / 4]))
    ax.set_ylim(
        ([vect.total_bounds[1] - height / 4, vect.total_bounds[3] + height / 4])
    )
    rasterio.plot.show(rast, ax=ax)
    vect.plot(facecolor="none", edgecolor="none", ax=ax)
    plt.show()
    print(datetime.now() - s)


with session_scope(commit=True, database=db, echo=False) as sess:
    grd_list = get_unclassified_polys(sess)
    print(f"Loading first GRD")
    download_grds([grd_list[0]], separate_process=False)

    for i, grd in enumerate(grd_list):
        if i < len(grd_list):
            print("Loading next GRD in background")
            download_grds([grd_list[i + 1]], separate_process=True)

        vector_path = outpath / f"vectors/{grd.pid}.geojson"
        raster_path = outpath / f"rasters/{grd.pid}.tiff"
        warped_path = raster_path.with_name("warped.tif")

        while not raster_path.exists(): # This crashes if the file isn't on S3!
            pass

        print("Warping image")
        gdal.Warp(
            destNameOrDestDS=str(warped_path),
            srcDSOrSrcDSTab=str(raster_path),
            format="GTiff",
        )

        rast = rasterio.open(warped_path)
        vect = geopandas.read_file(vector_path)
        # gdf = geopandas.GeoDataFrame()
        # d = {"geometry": [to_shape(poly.geometry) for poly in grd.inferences[-1].posi_polys]}
        # vect = geopandas.GeoDataFrame(d)

        print("Plotting image")
        plot4(rast, vect)
        print(grd.pid)
        # print(grd.calc_eezs())

        category = "No Category Assigned"
        while category.lower() not in ["v", "i", "n", "a", " ", "q"]:
            category = input(
                "What class is this polygon? \
                \n V = Vessel \
                \n I = Infrastructure \
                \n N = No Oil\
                \n A = Ambiguous \
                \n [Blank] = Skip \
                \n Q = quit"
            )
        if category.lower() == "q":
            break
        elif category.lower() != " ":
            slick = grd.inferences[-1].posi_polys[0].slick
            slick.class_int = class_int_dict[category.lower()]
            processed_pids += [[grd.id, grd.pid]]

shutil.rmtree(raster_path.parent)
shutil.rmtree(vector_path.parent)

with open(f"{path_config.LOCAL_DIR}temp/outputs/manually_classified.csv", "a") as f:
    writer = csv.writer(f)
    for proc in processed_pids:
        writer.writerow(proc + [date.today()])

# %%