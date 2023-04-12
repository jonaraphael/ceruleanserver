"""
Utilities for working with Google Earth Engine
"""

import datetime

import ee
ee.Initialize()


def get_s1_tile_layer(collect_time: datetime.datetime, scene_id: str):
    """
    Get a Sentinel-1 tile layer for a given time and GEE scene ID

    Args:
        collect_time (datetime): The time of the image collection
        scene_id (str): The GEE scene ID
    Returns:
        (str, dict): The tile layer URL and the footprint
    """
    # create a one hour time window on each side from the collect time
    start_time = collect_time - datetime.timedelta(hours=1)
    end_time = collect_time + datetime.timedelta(hours=1)
    
    # grab S1 image collection
    s1_ic = ee.ImageCollection('COPERNICUS/S1_GRD')
    s1_ic = s1_ic.filterDate(start_time, end_time)
    s1_ic = s1_ic.filter(ee.Filter.eq('instrumentMode', 'IW'))

    # check for VV/VH
    s1_ic = s1_ic.filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
    s1_ic = s1_ic.filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VH'))

    # mosaic the collection
    # this will give us neighboring scenes if they exist
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
