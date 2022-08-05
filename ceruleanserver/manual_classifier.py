#%%
%matplotlib inline

db = "cerulean"
current_class = None # Human labeled class
start_date = "2020-01-01"
end_date = "2023-01-01"
# Default: Order by polsby-popper polygon Perimeter^2/Area
set_by_area = False # Order by absolute area instead
set_by_linear = True # Order by polsby-popper multiplied by rectangular fill-factor
use_qgis = False # Display downloaded images in QGIS rather than in the iPython window
use_ee = True # Display streamed images in from EE rather than downloading

manual_pids = [
]

from db_connection import session_scope
from utils.s3 import sync_grds_and_vecs
from data_objects import Slick_Ext, Grd_Ext, Inference_Ext, Posi_Poly_Ext
from sqlalchemy import func, desc, cast
from pathlib import Path
from configs import path_config
from datetime import date
import csv
from geoalchemy2 import Geography, Geometry
import shutil
from IPython.display import clear_output
import pandas.io.clipboard as pyperclip
from subprocess import call
from ee_viewer import context_map

processed_pids = []
outpath = Path(path_config.LOCAL_DIR) / "temp" / "outputs"
class_int_dict = {
    "n": 0, # Not Oil
    "v": 1, # Vessel
    "f": 2, # Fixed Source
    "a": 3, # Ambiguous
    " ": None, # Skip
    "q": None, # Save and Quit

    # Expert Review
    "4": 4, # Vessel: Old
    "18": 18, # Vessel: Recent
    "19": 19, # Vessel: Adjacent
    "6": 6, # Infrastructure
    # "7": 7, # Shipwreck
    "8": 8, # Anchorage
    "9": 9, # Natural Seep
    "10": 10, # Slack Water
    "11": 11, # Weather
    "12": 12, # Convergence Zone
    "13": 13, # Ice
    "14": 14, # Wake
    "15": 15, # Internal Waves
    "16": 16, # Unknown
    "17": 17, # Land/Coast/Shore/Reef
    "20": 20, # Duplicate GRD
}

def get_unclassified_polys(sess, by_area, by_linear, start=None, end=None, pids=[]):
    q = (
        sess.query(Grd_Ext)
        .join(Inference_Ext)
        .join(Posi_Poly_Ext)
        .join(Slick_Ext)
        .filter(Slick_Ext.class_int == current_class)
    )
    if start:
        q = q.filter(Grd_Ext.starttime >= start)
    if end:
        q=q.filter(Grd_Ext.starttime < end)
    if by_area:
        q = q.order_by(desc(func.ST_Area(Posi_Poly_Ext.geometry)))
    elif by_linear:
        q = q.order_by(
            desc(
                1
                / func.ST_Area(cast(func.ST_OrientedEnvelope(cast(Posi_Poly_Ext.geometry, Geometry)), Geography))
                * func.ST_Perimeter(Posi_Poly_Ext.geometry)
                * func.ST_Perimeter(Posi_Poly_Ext.geometry)
            )
        )
    else:
        q = q.order_by(
            desc(
                1
                / func.ST_Area(Posi_Poly_Ext.geometry)
                * func.ST_Perimeter(Posi_Poly_Ext.geometry)
                * func.ST_Perimeter(Posi_Poly_Ext.geometry)
            )
        )
    if pids:
        q=q.filter(Grd_Ext.pid.in_(pids))

    return q.distinct().all()


def download_grds(grds, separate_process):
    sync_grds_and_vecs([grd.pid for grd in grds], separate_process, overwrite=True, download_grds=not use_ee)

with session_scope(commit=True, database=db, echo=False) as sess:
    if manual_pids:
        grd_list = get_unclassified_polys(sess, set_by_area, set_by_linear, pids=manual_pids)
    else:
        grd_list = get_unclassified_polys(sess, set_by_area, set_by_linear, start_date, end_date)
    print("Found", len(grd_list), "GRDs")
    print(f"Loading first GRD")
    download_grds([grd_list[0]], separate_process=False)

    for i, grd in enumerate(grd_list):
        category = "No Category Assigned"
        if i < len(grd_list)-1:
            print("Loading next GRD in background")
            download_grds([grd_list[i + 1]], separate_process=True)

        clear_output(wait=True)
        pyperclip.copy(grd.pid) # Store grd.pid in the copy/paste clipboard

        raster_path = outpath / f"rasters/{grd.pid}.tiff"
        vector_path = outpath / f"vectors/{grd.pid}.geojson"
        if use_qgis:
            while not (raster_path.exists() and vector_path.exists()):  # This crashes if we haven't downloaded the files! XXX Crashes if not on S3
                pass
            call(['open', '-a' '/Applications/QGIS3.8.app', str(raster_path)])
            call(['open', '-a' '/Applications/QGIS3.8.app', str(vector_path)])
        elif use_ee:
            while not vector_path.exists():  # This crashes if we haven't downloaded the file! XXX Crashes if not on S3
                pass
            found = context_map([grd.pid])
            if not found: 
                category = " "

        while category.lower() not in class_int_dict:
            category = input("""Class: (V)essel, (F)ixed, (A)mbiguous, (N)one || Zoom: (I)nset, (O)utset || (Q)uit and save . . . . . . . .""")
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

shutil.rmtree(outpath / "rasters", ignore_errors=True)
shutil.rmtree(outpath / "vectors", ignore_errors=True)
# %%
