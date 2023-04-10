"""
Utilities and helper functions for the oil slick Leaflet map
"""

from datetime import datetime, timedelta
import math

import centerline.geometry
from fastdtw import fastdtw
import gdstk
import geopandas as gpd
import movingpandas as mpd
import numpy as np
import pandas as pd
import scipy.interpolate
import scipy.spatial.distance
import shapely.geometry
import shapely.ops
from shapely import frechet_distance

import ee
ee.Initialize()



def get_slick_curve_direction(curve: shapely.geometry.LineString):
    """
    Compute the direction of a slick curve
    North is 0 degrees, moving clockwise
    """
    start_point, end_point = curve.coords[0], curve.coords[-1]
    dx = end_point[0] - start_point[0]
    dy = end_point[1] - start_point[1]
    angle = math.degrees(math.atan2(dy, dx))
    direction = (90 - angle) % 360
    return direction


def compute_dtw_distance(traj: mpd.Trajectory, curve: shapely.geometry.LineString):
    """
    Compute the DTW distance between a trajectory and a curve
    """
    # convert the trajectory to a numpy array
    traj_coords = np.array(traj.to_traj_gdf().iloc[0].geometry.coords)

    # convert the curve to a numpy array
    curve_coords = np.array(curve.coords)

    # compute the DTW distance between the trajectory and the curve
    dist, _ = fastdtw(traj_coords, curve_coords, dist=scipy.spatial.distance.euclidean)
    return dist


def compute_frechet_distance(traj: mpd.Trajectory, curve: shapely.geometry.LineString):
    """
    Compute the frechet distance between a trajectory and a slick curve
    """
    # get the trajectory coordinates as points in descending time order from collect
    traj_gdf = traj.to_point_gdf().sort_values(by='timestamp', ascending=False)

    # take the points and put them in a linestring
    traj_line = shapely.geometry.LineString(traj_gdf.geometry)

    # get the first and last points of the slick curve
    first_point = shapely.geometry.Point(curve.coords[0])
    last_point = shapely.geometry.Point(curve.coords[-1])

    # compute the distance from these points to the start of the trajectory
    first_dist = first_point.distance(shapely.geometry.Point(traj_line.coords[0]))
    last_dist = last_point.distance(shapely.geometry.Point(traj_line.coords[0]))
    if last_dist < first_dist:
        # reverse the slick curve
        curve = shapely.geometry.LineString(list(curve.coords)[::-1])

    # for every point in the curve, find the closest trajectory point and store it off
    traj_points = list()
    for curve_point in curve.coords:
        # compute the distance between this point and every point in the trajectory
        these_distances = list()
        for traj_point in traj_line.coords:
            dist = shapely.geometry.Point(curve_point).distance(shapely.geometry.Point(traj_point))
            these_distances.append(dist)

        closest_distance = min(these_distances)
        closest_idx = these_distances.index(closest_distance)
        traj_points.append(shapely.geometry.Point(traj_line.coords[closest_idx]))

    # compute the frechet distance
    traj_line_clip = shapely.geometry.LineString(traj_points)
    dist = frechet_distance(traj_line_clip, curve)
    return dist


def compute_mean_euclidean_distance(traj: mpd.Trajectory, slick: shapely.geometry.Polygon):
    """
    Compute the mean of the shortest distance from the exterior of every point
    in an oil slick polygon to the trajectory
    """
    # convert trajectory to a linestring
    traj_points = traj.to_point_gdf()

    # iterate over exterior points in the polygon
    distances = list()
    for slick_point in slick.exterior.coords:
        # compute the distance between this point and every point in the trajectory
        these_distances = list()
        for traj_point in traj_points.geometry:
            dist = shapely.geometry.Point(slick_point).distance(traj_point)
            these_distances.append(dist)

        closest_distance = min(these_distances)
        distances.append(closest_distance)

    # get mean distance
    mean_dist = np.mean(distances)
    return mean_dist


def associate_by_overlap_weighted(ais: mpd.TrajectoryCollection, 
                                  buffered: gpd.GeoDataFrame,
                                  weighted: list,
                                  slick: gpd.GeoDataFrame,
                                  curves: gpd.GeoDataFrame):
    """
    Measure association by computing the overlap between weighted AIS trajectories and slicks
    """
    
    # only consider trajectories that intersect slick detections
    ais_filt = list()
    weighted_filt = list()
    buffered_filt = list()
    for idx, t in enumerate(ais):
        w = weighted[idx]
        b = buffered.iloc[idx]

        # spatially join the weighted trajectory to the slick
        b_gdf = gpd.GeoDataFrame(index=[0], geometry=[b.geometry], crs=slick.crs)
        matches = gpd.sjoin(b_gdf, slick, how="inner", predicate="intersects")
        if matches.empty:
            continue
        else:
            ais_filt.append(t)
            weighted_filt.append(w)
            buffered_filt.append(b.geometry)

    associations = list()
    if not weighted_filt: # no associations found
        # return a geodataframe that has the same format as the usual case
        entry = dict()
        entry['geometry'] = slick.iloc[0].geometry
        entry['slick_index'] = None
        entry['slick_size'] = None
        entry['temporal_score'] = None
        entry['overlap_score'] = None
        entry['frechet_dist'] = None
        entry['total_score'] = None
        entry['traj_id'] = None
        associations.append(entry)
        associations = gpd.GeoDataFrame(associations, crs=slick.crs)
        return associations

    # create trajectory collection from filtered trajectories
    ais_filt = mpd.TrajectoryCollection(ais_filt)

    # iterate over each slick
    for idx in range(len(slick)):
        s = slick.iloc[idx]
        c = curves.iloc[idx]
        
        # iterate over filtered trajectories
        for t, w, b in zip(ais_filt, weighted_filt, buffered_filt):
            # spatially join the weighted trajectory to the slick
            s_gdf = gpd.GeoDataFrame(index=[0], geometry=[s.geometry], crs=slick.crs)
            matches = gpd.sjoin(w, s_gdf, how="inner", predicate="intersects")
            
            if matches.empty:
                temporal_score = 0.0
            else:
                # out of the resulting matches, pick the highest weight
                temporal_score = matches['weight'].max()

            # compute percent overlap between buffered trajectory and slick
            overlap_score = s.geometry.intersection(b).area / s.geometry.area

            # compute frechet distance between trajectory and slick curve
            frechet_dist = compute_frechet_distance(t, c.geometry)

            total_score = 1.5*temporal_score + 1.0*overlap_score + 1000.0/frechet_dist
            
            entry = dict()
            entry['geometry'] = s.geometry
            entry['slick_index'] = idx
            entry['slick_size'] = s.geometry.area
            entry['temporal_score'] = temporal_score
            entry['overlap_score'] = overlap_score
            entry['frechet_dist'] = frechet_dist
            entry['total_score'] = total_score
            entry['traj_id'] = t.id
            
            associations.append(entry)

    associations = gpd.GeoDataFrame(associations, crs=slick.crs)
    
    return associations


def associate_by_overlap(ais: mpd.TrajectoryCollection, slick: gpd.GeoDataFrame):
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


def sample_points(gdf: gpd.GeoDataFrame, num_samples: int = 50):
    """
    Randomly sample points from a GeoDataFrame
    """
    x_min, y_min, x_max, y_max = gdf.total_bounds

    points = list()
    while len(points) < num_samples:
        x = np.random.uniform(x_min, x_max)
        y = np.random.uniform(y_min, y_max)

        point = shapely.geometry.Point(x, y)
        if point.within(gdf.unary_union):
            points.append(point)

    points = gpd.GeoSeries(points, crs=gdf.crs)

    return points


def associate_by_distance(ais: mpd.TrajectoryCollection, slick: gpd.GeoDataFrame, num_samples: int = 50):
    """
    This is a slightly varied implementation of Jona's algorithm

    Randomly sample points within the slick
    For each point, find the AIS trajectory with the lowest MAE by distance
    """
    # randomly sample points in the slick
    points = sample_points(slick, num_samples)

    # for every trajectory, compute the mean distance to the sampled points
    mean_distances = list()
    for trajectory in ais:
        # trajectory.distance() calculates the shortest distance from the trajectory to each point
        mean_distance = trajectory.distance(points).mean()
        mean_distances.append(mean_distance)

    # add to trajectory dataframe
    traj_gdf = ais.to_traj_gdf()
    traj_gdf['mean_distance'] = mean_distances

    # sort by mean distance
    traj_gdf = traj_gdf.sort_values(by="mean_distance", ascending=True)
    return traj_gdf


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
            # build trajectory 
            traj = mpd.Trajectory(
                df=group,
                traj_id=ssvid,
                t="timestamp"
            )

            # interpolate/extrapolate to times in time_vec
            times = list()
            positions = list()
            for t in time_vec:
                pos = traj.interpolate_position_at(t)
                times.append(t)
                positions.append(pos)

            # store as trajectory
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


def slicks_to_curves(slicks: gpd.GeoDataFrame, 
                     buf_size: int = 2000, 
                     interp_dist: int = 200,
                     smoothing_factor: float = 1e9):
    """
    From a set of oil slick detections, estimate curves that go through the detections
    This process transforms a set of slick detections into LineStrings for each detection
    """
    # clean up the slick detections by dilation followed by erosion
    # this process can merge some polygons but not others, depending on proximity
    slicks_clean = slicks.copy()
    slicks_clean.geometry = slicks_clean.geometry.buffer(buf_size).buffer(-buf_size)

    # split slicks into individual polygons
    slicks_clean = slicks_clean.explode(ignore_index=True, index_parts=False)

    # find a centerline through detections
    slick_curves = list()
    for idx, row in slicks_clean.iterrows():
        # create centerline -> MultiLineString
        try:
            cl = centerline.geometry.Centerline(row.geometry, interpolation_distance=interp_dist)
        except:
            # sometimes the voronoi polygonization fails
            # in this case, just fit a a simple line from the start to the end
            exterior_coords = row.geometry.exterior.coords
            start_point = exterior_coords[0]
            end_point = exterior_coords[-1]
            curve = shapely.geometry.LineString([start_point, end_point])
            slick_curves.append(curve)
            continue

        # grab coordinates from centerline
        x = list()
        y = list()
        if type(cl.geometry) == shapely.geometry.MultiLineString:
            # iterate through each linestring
            for geom in cl.geometry.geoms:
                x.extend(geom.coords.xy[0])
                y.extend(geom.coords.xy[1])
        else:
            x.extend(cl.geometry.coords.xy[0])
            y.extend(cl.geometry.coords.xy[1])

        # sort coordinates in both X and Y directions
        coords = [(xc, yc) for xc, yc in zip(x, y)]
        coords_sort_x = sorted(coords, key=lambda c: c[0])
        coords_sort_y = sorted(coords, key=lambda c: c[1])

        # remove coordinate duplicates, preserving sorted order
        coords_seen_x = set()
        coords_unique_x = list()
        for c in coords_sort_x:
            if c not in coords_seen_x:
                coords_unique_x.append(c)
                coords_seen_x.add(c)
        
        coords_seen_y = set()
        coords_unique_y = list()
        for c in coords_sort_y:
            if c not in coords_seen_y:
                coords_unique_y.append(c)
                coords_seen_y.add(c)
        
        # grab x and y coordinates for spline fit
        x_fit_sort_x = [c[0] for c in coords_unique_x]
        x_fit_sort_y = [c[0] for c in coords_unique_y]
        y_fit_sort_x = [c[1] for c in coords_unique_x]
        y_fit_sort_y = [c[1] for c in coords_unique_y]

        # fit a B-spline to the centerline
        tck_sort_x, fp_sort_x, _, _ = scipy.interpolate.splrep(
            x_fit_sort_x, 
            y_fit_sort_x, 
            k=3, 
            s=smoothing_factor, 
            full_output=True
        )
        tck_sort_y, fp_sort_y, _, _ = scipy.interpolate.splrep(
            y_fit_sort_y,
            x_fit_sort_y,
            k=3, 
            s=smoothing_factor, 
            full_output=True
        )

        # choose the spline that has the lowest fit error
        if fp_sort_x <= fp_sort_y:
            tck = tck_sort_x
            x_fit = x_fit_sort_x
            y_fit = y_fit_sort_x
            
            num_points = max(round((x_fit[-1] - x_fit[0]) / 100), 5)
            x_new = np.linspace(x_fit[0], x_fit[-1], 10)
            y_new = scipy.interpolate.BSpline(*tck)(x_new)
        else:
            tck = tck_sort_y
            x_fit = x_fit_sort_y
            y_fit = y_fit_sort_y
            
            num_points = max(round((y_fit[-1] - y_fit[0]) / 100), 5)
            y_new = np.linspace(y_fit[0], y_fit[-1], num_points)
            x_new = scipy.interpolate.BSpline(*tck)(y_new)

        # store as LineString
        curve = shapely.geometry.LineString(zip(x_new, y_new))
        slick_curves.append(curve)

    slick_curves = gpd.GeoDataFrame(geometry=slick_curves, crs=slicks_clean.crs)
    return slicks_clean, slick_curves


def buffer_trajectories(ais: mpd.TrajectoryCollection,
                        buf_vec: np.ndarray,
                        weight_vec: np.ndarray) -> gpd.GeoDataFrame:
    """
    Build conic buffers around each trajectory
    Buffer is narrowest at the start and widest at the end
    Weight is highest at the start and lowest at the end
    """
    ais_buf = list()
    ais_weighted = list()
    for traj in ais:
        # grab points
        points = traj.to_point_gdf()
        points = points.sort_values(by="timestamp", ascending=False)

        # create buffered circles at points
        ps = list()
        for idx, buffer in enumerate(buf_vec):
            ps.append(points.iloc[idx].geometry.buffer(buffer))
        
        # create convex hulls from circles
        n = range(len(ps) - 1)
        convex_hulls = [shapely.geometry.MultiPolygon([ps[i], ps[i+1]]).convex_hull for i in n]

        # weight convex hulls
        weighted = list()
        for c, w in zip(convex_hulls, weight_vec):
            entry = dict()
            entry['geometry'] = c
            entry['weight'] = w
            weighted.append(entry)
        weighted = gpd.GeoDataFrame(weighted, crs=traj.crs)
        ais_weighted.append(weighted)

        # create polygon from hulls
        poly = shapely.ops.unary_union(convex_hulls)
        ais_buf.append(poly)

    ais_buf = gpd.GeoDataFrame(geometry=ais_buf, crs=traj.crs)
    return ais_buf, ais_weighted


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

