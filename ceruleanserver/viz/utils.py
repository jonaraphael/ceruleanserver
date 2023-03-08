"""
Utilities and helper functions for the oil slick Leaflet map
"""

from datetime import datetime, timedelta

import geopandas as gpd
import movingpandas as mpd
import pandas as pd
import shapely.geometry

import ee
ee.Initialize()


def associate_slicks_to_ais(ais: mpd.TrajectoryCollection, slick: gpd.GeoDataFrame):
    """
    Associate oil slicks to AIS trajectories
    """
    # buffer the AIS trajectories by 2km on each side
    ais_buf = ais.to_traj_gdf().copy()
    ais_buf.geometry = ais_buf.geometry.buffer(2000)

    # find the slicks that intersect with the AIS trajectories
    slick_ais = gpd.sjoin(ais_buf, slick, how="inner", predicate="intersects")
    
    # if there are no matches, there are no associations to report
    if slick_ais.empty:
        return slick_ais

    # for every match, calculate the percent of the slick that is within the AIS trajectory
    slick_ais["overlap"] = slick_ais.apply(
        lambda row: slick.geometry.intersection(row.geometry).area / slick.geometry.area,
        axis=1
    )

    # sort by overlap
    slick_ais = slick_ais.sort_values(by="overlap", ascending=False)
    return slick_ais


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


def ais_points_to_trajectories(ais: gpd.GeoDataFrame, time_vec: pd.DatetimeIndex):
    """
    Convert a set of AIS points into trajectories, grouped by ssvid
    For each trajectory, interpolate the position along a time vector
    """
    ais_trajectories = list()
    for ssvid, group in ais.groupby('ssvid'):
        if len(group) > 1: # ignore single points
            traj = mpd.Trajectory(
                df=group,
                traj_id=ssvid,
                t="timestamp"
            )

            times = list()
            positions = list()
            for t in time_vec:
                pos = traj.interpolate_position_at(t)
                times.append(t)
                positions.append(pos)

            interpolated_traj = mpd.Trajectory(
                df=gpd.GeoDataFrame(
                    {
                        "timestamp": times,
                        "geometry": positions
                    },
                    crs=ais.crs
                ),
                traj_id=ssvid,
                t="timestamp"
            )

            ais_trajectories.append(interpolated_traj)

    return mpd.TrajectoryCollection(ais_trajectories)


def get_s1_tile_layer(collect_time: datetime, basename: str):
    """
    Get a Sentinel-1 tile layer for a given time and basename
    """
    start_time = collect_time - timedelta(hours=1)
    end_time = collect_time + timedelta(hours=1)

    s1_ic = ee.ImageCollection('COPERNICUS/S1_GRD')
    s1_ic = s1_ic.filterDate(start_time, end_time)
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









