"""
Utilities and helper functions for the oil slick Leaflet map
"""

from datetime import datetime, timedelta

import geopandas as gpd
import shapely.geometry

import ee
ee.Initialize()


def ais_points_to_lines(ais: gpd.GeoDataFrame):
    """
    Convert a set of AIS points into lines, grouped by ssvid
    """
    ais_lines = list()
    for ssvid, group in ais.groupby('ssvid'):
        if len(group) > 1:
            ls = shapely.geometry.LineString(group.geometry.tolist())
        else:
            ls = group.iloc[0].geometry

        entry = dict()
        entry["ssvid"] = ssvid
        entry["geometry"] = ls
        ais_lines.append(entry)

    ais_lines = gpd.GeoDataFrame(ais_lines, crs='EPSG:4326')
    return ais_lines


def get_s1_tile_layer(collect_time: datetime, basename: str):
    start_time = collect_time - timedelta(days=1)
    end_time = collect_time + timedelta(days=1)

    s1_ic = ee.ImageCollection('COPERNICUS/S1_GRD')
    s1_ic = s1_ic.filterDate(start_time.strftime('%Y-%m-%d'), end_time.strftime('%Y-%m-%d'))
    s1_ic = s1_ic.filter(ee.Filter.eq('instrumentMode', 'IW'))

    # check for VV/VH
    s1_ic = s1_ic.filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
    s1_ic = s1_ic.filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VH'))

    # mosaic the collection
    s1_img = s1_ic.mosaic()

    # get the footprint from the basename
    try: # for some reason not all S1 IDs seem to exist in GEE, but most do
        base_img = ee.Image(f"COPERNICUS/S1_GRD/{basename}")
        footprint = base_img.geometry().getInfo()
    except:
        footprint = dict()

    # return a tile layer suitable for visualization
    vis_params = {'min': -30, 'max': 1, 'bands': ['VV']}
    map_id = s1_img.getMapId(vis_params)
    
    return map_id['tile_fetcher'].url_format, footprint









