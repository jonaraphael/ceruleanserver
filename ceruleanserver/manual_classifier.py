#%%
%matplotlib inline

from db_connection import session_scope
from utils.s3 import sync_grds_and_vecs
from data_objects import Slick_Ext, Grd_Ext, Inference_Ext, Posi_Poly_Ext
from sqlalchemy import func, desc, cast
from pathlib import Path
from configs import path_config
import geopandas
import rasterio
import matplotlib.pyplot as plt
import rasterio.plot
from osgeo import gdal
from datetime import datetime, date
import csv
from geoalchemy2.shape import to_shape
from geoalchemy2 import Geography
import shutil
from IPython.display import clear_output
import matplotlib.patches as patches

db = "cerulean"
set_by_area = False
current_class = 1

processed_pids = []
outpath = Path(path_config.LOCAL_DIR) / "temp" / "outputs"
class_int_dict = {
    "n": 0,
    "v": 1,
    "f": 2,
    "a": 3,
    " ": None, # Skip
    "q": None, # Save and Quit
}
ocean_path = Path(path_config.LOCAL_DIR) / "aux_files" / "OceanGeoJSON_lowres.geojson"
ocean = geopandas.read_file(ocean_path)


def get_unclassified_polys(sess, by_area=False):
    q = (
        sess.query(Grd_Ext)
        .join(Inference_Ext)
        .join(Posi_Poly_Ext)
        .join(Slick_Ext)
        .filter(Slick_Ext.class_int == current_class)
    )
    if by_area:
        q = q.order_by(desc(func.ST_Area(Posi_Poly_Ext.geometry)))
    else:
        q = q.order_by(
            desc(
                1
                / func.ST_Area(Posi_Poly_Ext.geometry)
                * func.ST_Perimeter(Posi_Poly_Ext.geometry)
                * func.ST_Perimeter(Posi_Poly_Ext.geometry)
            )
        )

    return q.distinct().all()


def download_grds(grds, separate_process):
    sync_grds_and_vecs([grd.pid for grd in grds], separate_process)

def plot_super(rast=None, vect=None, zoom=None, box_factor=None, patch=None, edgecolor="red", facecolor="none", vect_line=1):
    size = 10 * (zoom or 1)
    fig, ax = plt.subplots(figsize=(size,size))
    if rast is not None:
        rasterio.plot.show(rast, ax=ax)
    if vect is not None:
        vect.plot(facecolor=facecolor, edgecolor=edgecolor, linewidth=vect_line, ax=ax)
        if box_factor:
            w = abs(vect.total_bounds[2] - vect.total_bounds[0]) * box_factor / 2
            h = abs(vect.total_bounds[3] - vect.total_bounds[1]) * box_factor / 2
            ax.set_xlim([vect.total_bounds[0] - w, vect.total_bounds[2] + w])
            ax.set_ylim([vect.total_bounds[1] - h, vect.total_bounds[3] + h])
    if patch is not None:
        ax.add_patch(rast_to_patch(patch))
        if box_factor:
            w = abs(patch.bounds[2] - patch.bounds[0]) * box_factor / 2
            h = abs(patch.bounds[3] - patch.bounds[1]) * box_factor / 2
            ax.set_xlim([patch.bounds[0] - w, patch.bounds[2] + w])
            ax.set_ylim([patch.bounds[1] - h, patch.bounds[3] + h])
    plt.show()
    print(".")

def rast_to_patch(rast):
    bbox = rast.bounds
    res = patches.Rectangle(
        (bbox[0], bbox[1]),
        bbox[2] - bbox[0],
        bbox[3] - bbox[1],
        alpha=.5,
        linewidth=2,
        edgecolor="red",
        facecolor="red",
    )
    return res


with session_scope(commit=True, database=db, echo=False) as sess:
    grd_list = get_unclassified_polys(sess, set_by_area)
    print(f"Loading first GRD")
    download_grds([grd_list[0]], separate_process=False)

    for i, grd in enumerate(grd_list):
        if i < len(grd_list):
            print("Loading next GRD in background")
            download_grds([grd_list[i + 1]], separate_process=True)

        raster_path = outpath / f"rasters/{grd.pid}.tiff"
        warped_path = raster_path.with_name("warped.tif")

        while not raster_path.exists():  # This crashes if the file isn't on S3!
            pass

        print("Warping image")
        gdal.Warp(
            destNameOrDestDS=str(warped_path),
            srcDSOrSrcDSTab=str(raster_path),
            format="GTiff",
        )

        rast = rasterio.open(warped_path)

        poly_shapes = [to_shape(poly.geometry) for poly in grd.inferences[-1].posi_polys]
        vect = geopandas.GeoDataFrame({"geometry": poly_shapes})
        # largest = vect.iloc[[vect.length.values.argmax()]]

        print("Plotting image")
        clear_output(wait=True)
        zoom_level = 2
        plot_super(vect=ocean, patch=rast, edgecolor="black", facecolor="xkcd:light blue", box_factor=30)
        plot_super(rast=rast, vect=vect)
        plot_super(rast=rast)
        plot_super(rast=rast, vect=vect, vect_line=.2, box_factor=.25, zoom=zoom_level)
        print(grd.pid)
        # print(grd.calc_eezs())

        category = "No Category Assigned"
        while category.lower() not in class_int_dict:
            category = input("""
                What class is this polygon? . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .\
                V = Vessel <><><> \
                F = Fixed (Infrastructure or Natural) <><><> \
                N = No Oil <><><> \
                A = Ambiguous <><><> \
                I = Zoom IN <><><> \
                O = Zoom OUT <><><> \
                [Blank] = Skip <><><> \
                Q = Quit and Save Progress
            """)
            if category.lower() == "i":
                zoom_level += 1
                plot_super(rast=rast, vect=vect, vect_line=.1, box_factor=1, zoom=zoom_level)
            elif category.lower() == "o":
                zoom_level += 1
                plot_super(rast=rast, vect=vect, vect_line=.1, zoom=zoom_level)
        if category.lower() == "q":
            break
        elif category.lower() != " ":
            slick = grd.inferences[-1].posi_polys[0].slick
            slick.class_int = class_int_dict[category.lower()]
            processed_pids += [[grd.id, grd.pid]]

with open(f"{path_config.LOCAL_DIR}temp/outputs/manually_classified.csv", "a") as f:
    writer = csv.writer(f)
    for proc in processed_pids:
        writer.writerow(proc + [date.today()])

shutil.rmtree(raster_path.parent)
shutil.rmtree(outpath / "vectors")
# %%
