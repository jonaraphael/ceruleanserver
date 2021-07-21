#%%
from datetime import datetime, timedelta
import pandas_gbq
from pathlib import Path
from configs import path_config
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, shape, Polygon, MultiPolygon, MultiPoint
from shapely.ops import split
from ml.vector_processing import geojson_to_shapely_multi
import numpy as np

# Load from config
project_id = "world-fishing-827"
in_format = "%Y%m%dT%H%M%S"
d_format = "%Y-%m-%d"
t_format = "%Y-%m-%d %H:%M:%S"

fdir = Path(path_config.LOCAL_DIR)/"temp/outputs/"
ais_dir = fdir/"ais"
vect_dir = fdir/"vectors"


def download_ais(t_stamp, poly, back_window, forward_window):
    sql = f"""
        SELECT * FROM(
        SELECT 
        seg.ssvid as ssvid, 
        seg.timestamp as timestamp, 
        seg.lon as lon, 
        seg.lat as lat,
        ves.ais_identity.shipname_mostcommon.value as shipname,
        ves.ais_identity.shiptype[SAFE_OFFSET(0)].value as shiptype,
        ves.best.best_flag as flag,
        ves.best.best_vessel_class as best_shiptype
        FROM 
        `world-fishing-827.gfw_research.pipe_v20201001` as seg
        JOIN 
        `world-fishing-827.gfw_research.vi_ssvid_v20201209` as ves
        ON seg.ssvid = ves.ssvid
        
        WHERE
        seg._PARTITIONTIME >= '{datetime.strftime(t_stamp-timedelta(hours=back_window), d_format)}'
        AND seg._PARTITIONTIME <= '{datetime.strftime(t_stamp+timedelta(hours=forward_window), d_format)}'
        AND seg.timestamp >= '{datetime.strftime(t_stamp-timedelta(hours=back_window), t_format)}'
        AND seg.timestamp <= '{datetime.strftime(t_stamp+timedelta(hours=forward_window), t_format)}'
        AND ST_COVEREDBY(ST_GEOGPOINT(seg.lon, seg.lat), ST_GeogFromText('{poly}'))

        ORDER BY
        seg.timestamp DESC
        )
        ORDER BY 
        ssvid, timestamp
    """
    return pandas_gbq.read_gbq(sql, project_id=project_id)

def segment_splitter(curve):
    return list(map(LineString, zip(curve.coords[:-1], curve.coords[1:])))

def z_linestring_dist(z_point, z_line_string):
    return np.min([z_lineseg_dist(z_point, seg) for seg in segment_splitter(z_line_string)])
    # XXX This loop is SLOW how to get rid of it?

def z_lineseg_dist(z_point, z_line_segment):
    # from https://stackoverflow.com/questions/54442057/
    # XXX Why does this not return sqrt(3)/2 when using Point(0,0,1) and LineString(((0,0,0), (1,1,1)))???
    p = np.array(z_point.coords[0])
    a = np.array(z_line_segment.coords[0])
    b = np.array(z_line_segment.coords[1])
    if np.all(a == b):
        return np.linalg.norm(p - a)
    d = np.divide(b - a, np.linalg.norm(b - a)) # normalized tangent vector
    s = np.dot(a - p, d) # signed parallel distance components
    t = np.dot(p - b, d) # signed parallel distance components
    h = np.maximum.reduce([s, t, 0]) # clamped parallel distance
    c = np.cross(p - a, d) # perpendicular distance component, as before
    return np.hypot(h, np.linalg.norm(c)) # use hypot for Pythagoras to improve accuracy

def rectangle_from_pid(pid, buff):
    geojson_path = vect_dir/(pid+".geojson")
    with open(str(geojson_path)) as f:
        g = shape(json.load(f))
    return g.minimum_rotated_rectangle.buffer(buff).minimum_rotated_rectangle

def disc_from_point(longitude, latitude, buff):
    g = shape(Point(longitude, latitude))
    return g.buffer(buff)

def sync_ais_from_point_time(longitude, latitude, tstamp, back_window=12, forward_window=1.5, buff=.5):
    # tstamp must be a string in this format: "%Y%m%dT%H%M%S"
    ais_dir.mkdir(parents=True, exist_ok=True)    
    ais_path = ais_dir/(str(tstamp)+"_"+str(longitude)+"_"+str(latitude)+".geojson")
    if not ais_path.exists():
        disc = disc_from_point(longitude, latitude, buff)
        t_stamp = datetime.strptime(tstamp, in_format)
        df = download_ais(t_stamp, str(disc), back_window, forward_window)
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
        if len(df)>0:
            gdf.to_file(ais_path, driver="GeoJSON")
        else:
            print("No AIS data found for", ais_path.name)
    else:
        print("AIS already downloaded for", ais_path.name)    

def sync_ais_files(pids, back_window=12, forward_window=1.5, buff=.5):
    ais_dir.mkdir(parents=True, exist_ok=True)
    for pid in pids:
        ais_path = ais_dir/(pid+".geojson")
        geojson_path = vect_dir/(pid+".geojson")
        if not geojson_path.exists():
            print("No GeoJSON data found for", pid)
        elif not ais_path.exists():
            rect = rectangle_from_pid(pid, buff)
            time_from_pid = pid.split('_')[4]
            t_stamp = datetime.strptime(time_from_pid, in_format)
            df = download_ais(t_stamp, str(rect), back_window, forward_window)
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
            if len(df)>0:
                gdf.to_file(ais_path, driver="GeoJSON")
            else:
                print("No AIS data found for", pid)
        else:
            print("AIS already downloaded for", pid)

def sample_shape(polygon, size, overestimate=2):
    min_x, min_y, max_x, max_y = polygon.bounds
    ratio = polygon.area / polygon.envelope.area
    samples = np.random.uniform((min_x, min_y), (max_x, max_y), (int(size / ratio * overestimate), 2))
    multipoint = MultiPoint(samples)
    multipoint = multipoint.intersection(polygon)
    samples = np.array(multipoint)
    while samples.shape[0] < size:
        # emergency catch in case by bad luck we didn't get enough within the polygon
        samples = np.concatenate([samples, random_points_in_polygon(polygon, size, overestimate=overestimate)])
    return samples[np.random.choice(len(samples), size)]

def find_xy(p1, p2, z):
    x1, y1, z1 = p1
    x2, y2, z2 = p2
    if z2 < z1:
        return find_xy(p2, p1, z)

    x = np.interp(z, (z1, z2), (x1, x2))
    y = np.interp(z, (z1, z2), (y1, y2))

    return Point(x, y, z)

def cut(line, distance):
    # Cuts a line in two at a distance from its starting point
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i+1]),
                LineString(coords[i:])]
        if pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y, cp.z)]),
                LineString([(cp.x, cp.y, cp.z)] + coords[i:])]

def mae_ranking(pids, return_count=None, num_samples=50, vel=1):
    ## Buffer AIS linestrings to identify culprit
    # vel is a ratio from time to distance, used as to add a Z dimension to the AIS points for coincidence scoring
    for pid in pids:
        vect_path = vect_dir/(pid+".geojson")
        ais_path = ais_dir/(pid+".geojson")

        if not vect_path.exists():
            print("Could not find GeoJSON file for", pid)
        elif not ais_path.exists():
            print("Could not find AIS file for", pid)
        else:
            # Open the slick multipolygon
            with open(vect_path) as f:
                geom = json.load(f)
            slick_shape = shape(geom).buffer(0)
            sample_points = sample_shape(slick_shape, num_samples)
            slick_samples_gs = gpd.GeoSeries([Point(*s, 0) for s in sample_points])

            # Open the AIS data for the same GRD
            ais_df = gpd.read_file(ais_path).sort_values('timestamp')

            # Add DeltaTime column
            capture_timestamp = datetime.strptime(pid.split("_")[4], in_format)
            ais_df["delta_time"] = pd.to_datetime(ais_df["timestamp"], infer_datetime_format=True)-capture_timestamp
            ais_df["Z"] = ais_df["delta_time"].dt.total_seconds() / 3600 * vel / 111 # sec * hr/sec * km/hr * deg/km = deg

            # Find any solitary AIS broadcasts and duplicate them, because linestrings can't exist with just one point
            singletons = ~ais_df.duplicated(subset='ssvid', keep=False)
            duped_df = ais_df.loc[np.repeat(ais_df.index.values,singletons+1)]

            # Zip the coordinates into a point object and convert to a GeoData Frame
            geometry = [Point(xyz) for xyz in zip(duped_df.lon, duped_df.lat, duped_df.Z)]
            geo_df = gpd.GeoDataFrame(duped_df, geometry=geometry)

            # Create a new GDF that uses the SSVID to create linestrings
            ssvid_df = geo_df.groupby(['ssvid'])['geometry'].apply(lambda x: LineString(x.tolist()))
            ssvid_df = gpd.GeoDataFrame(ssvid_df, geometry='geometry')

            # Clip AIS tracks to data before image capture
            ssvid_df["ais_before_t0"] = ssvid_df["geometry"]
            for idx, vessel in ssvid_df["geometry"].iteritems():
                for p0, p1 in zip(vessel.coords[:], vessel.coords[1:]):
                    if p0[2] > 0: # No data points from before capture
                        ssvid_df["ais_before_t0"].loc[idx] = None
                        break
                    if p1[2] > 0: # Found the two datapoints that sandwich the capture Z=0
                        zero_intersection = find_xy(p0, p1, 0) # Find the XY where Z=0
                        segments = cut(vessel, vessel.project(zero_intersection))
                        ssvid_df["ais_before_t0"].loc[idx] = segments[0] if segments else None # Keep only negative segment                        
                        break

            # Calculate the Mean Absolute Error for each AIS Track
            # XXX Note that "if vessel else None" means that we are ignoring vessels that only broadcast AIS AFTER the image was captured (this is not ideal)
            ssvid_df['coinc_score'] = [slick_samples_gs.apply(func=z_linestring_dist, z_line_string=vessel).mean() if vessel else None for vessel in ssvid_df["ais_before_t0"]] # Mean Absolute Error

            # # Suggest Abstention
            # abstain_threshold = 0.05 # This is a threshold value that determines how often we abstain from blaming a vessel in the picture (default = 0.01). Raise the value to make it more likely to blame a vessel.
            # ssvid_df = ssvid_df.append(pd.Series({'coinc_score':abstain_threshold},name="^^^ Abstain Above^^^"))

            if return_count:
                print(ssvid_df.sort_values('coinc_score', ascending=False, na_position="first")["coinc_score"].tail(return_count))
            else:
                print(ssvid_df.sort_values('coinc_score', ascending=False, na_position="first")["coinc_score"])
            print(pid)

# %%

# # %%
# % matplotlib inline
# from pathlib import Path
# from configs import path_config
# import geopandas as gpd
# import numpy as np
# from shapely.geometry import Polygon#, Point, LineString, shape, MultiPolygon, MultiPoint

# fdir = Path(path_config.LOCAL_DIR)/"temp/outputs/"
# ais_dir = fdir/"ais"
# vect_dir = fdir/"vectors"

# def rect_to_bowtie(shapely_rectangle, stretch_factor=1, spread_factor=1):
#     pp = [np.array(pnt) for pnt in shapely_rectangle.exterior.coords[:-1]]
#     bowtie = []
#     for i, p_now in enumerate(pp):
#         # coords listed clockwise
#         p_prev = pp[(i-1)%4]
#         p_next = pp[(i+1)%4]
#         bowtie.append(p_now)
#         aspect_ratio = np.linalg.norm(p_now-p_prev) / np.linalg.norm(p_now-p_next)
#         first_new_point = p_now + (p_now - p_prev) * aspect_ratio * stretch_factor / 10 + (p_now-p_next) * aspect_ratio * stretch_factor * spread_factor / 10
#         bowtie.append(first_new_point)
#         second_new_point = p_now + (p_now - p_prev) * aspect_ratio * stretch_factor / 10 + (p_now-p_next) * (-1-aspect_ratio * stretch_factor * spread_factor / 10)
#         bowtie.append(second_new_point)
#     return Polygon(bowtie)

# def combine(dict_of_sets):
#     # This takes in a dictionary of sets, each pointing at other indices of the dictionary, 
#     # and recursively combines them if they overlap
#     # Example: Input = {1:{1,2}, 2:{2,4}, 3:{3}, 4:{4}} >>> {1:{1,2,4}, 2:{1,2,4}, 3:{3}, 4:{1,2,4}}
#     one_step_deeper = {ind:set([item for sublist in dict_of_sets[ind] for item in dict_of_sets[sublist]]) for ind in dict_of_sets}
#     if dict_of_sets == one_step_deeper: # No change, done with recursion
#         return one_step_deeper
#     else: 
#         return combine(one_step_deeper)
    
# pid = "S1A_IW_GRDH_1SDV_20200814T105729_20200814T105754_033902_03EE9F_3456"
# geojson_path = vect_dir/(pid+".geojson")
# orig_gdf = gpd.GeoDataFrame.from_file(geojson_path).explode()
# # TODO: Set Index equal to the PosiPoly_ID
# orig_gdf["polsby_fill"] = [poly.length**2/poly.minimum_rotated_rectangle.area for poly in orig_gdf['geometry']]
# poly_rects = gpd.GeoDataFrame({'geometry':[poly.minimum_rotated_rectangle for poly in orig_gdf['geometry']]})
# bowties = gpd.GeoDataFrame({"geometry":[rect_to_bowtie(rect) for rect in poly_rects["geometry"]]})

# overlaps = {i1:set([i2 for i2, p2 in bowties.itertuples() if p1.buffer(0).intersects(p2.buffer(0))]) for i1, p1 in bowties.itertuples()}
# slicks = combine(overlaps)
# slick_idxs = set([min(list(slicks[i])) for i in slicks])

# print(len(orig_gdf))
# for idx in slick_idxs:
#     print(idx)
#     orig_gdf.iloc[list(slicks[idx])].plot()
# bowties.plot()
# orig_gdf.plot()


# %%
