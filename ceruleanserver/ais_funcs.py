#%%
from datetime import datetime, timedelta, timezone
import pandas_gbq
from pathlib import Path
from configs import path_config
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, shape, MultiPoint
import numpy as np
from os import makedirs
import dateutil
import plotly.graph_objects as go
from tqdm import tqdm

# Load from config
project_id = "world-fishing-827"
in_format = "%Y%m%dT%H%M%S"
d_format = "%Y-%m-%d"
t_format = "%Y-%m-%d %H:%M:%S"

fdir = Path(path_config.LOCAL_DIR)/"temp/outputs/"
ais_dir = fdir/"_ais"
vect_dir = fdir/"_vectors"
coincidence_dir = fdir/"_coincidence"

pandas_gbq.context.project = project_id



def download_ais(t_stamp, back_window, forward_window, spire_only=False, sql=None, ssvids = None, poly = None):
    sql = sql or f"""
        SELECT * FROM(
        SELECT 
        seg.ssvid as ssvid, 
        seg.timestamp as timestamp, 
        seg.lon as lon, 
        seg.lat as lat,
        seg.course as course,
        seg.speed_knots as speed_knots,
        ves.ais_identity.shipname_mostcommon.value as shipname,
        ves.ais_identity.shiptype[SAFE_OFFSET(0)].value as shiptype,
        ves.best.best_flag as flag,
        ves.best.best_vessel_class as best_shiptype
        FROM 
        `world-fishing-827.gfw_research.pipe_v20201001` as seg
        LEFT JOIN 
        `world-fishing-827.gfw_research.vi_ssvid_v20201209` as ves
        ON seg.ssvid = ves.ssvid
        
        WHERE
        seg._PARTITIONTIME between '{datetime.strftime(t_stamp-timedelta(hours=back_window), d_format)}' AND '{datetime.strftime(t_stamp+timedelta(hours=forward_window), d_format)}'
        AND seg.timestamp between '{datetime.strftime(t_stamp-timedelta(hours=back_window), t_format)}' AND '{datetime.strftime(t_stamp+timedelta(hours=forward_window), t_format)}'
        """
    if poly:
        sql = sql + f"""
        AND ST_COVEREDBY(ST_GEOGPOINT(seg.lon, seg.lat), ST_GeogFromText('{poly}'))
        """
    if ssvids:
        sql = sql + f"""
        AND seg.ssvid in (
            '{"', '".join([str(ssvid) for ssvid in ssvids])}'
        )
        """
    if spire_only:
        sql = sql + f"""
        AND seg.source = 'spire'
        """
    sql = sql + f"""
        ORDER BY
        seg.timestamp DESC
        )
        ORDER BY 
        ssvid, timestamp
        """
    return pandas_gbq.read_gbq(sql, project_id=project_id)

def bulk_download_ais(filename, tstamp, ssvids, number_of_days = 1, single_file = True, add_max_dev=False):
    # This code is used to grab historic AIS tracks of specific SSVIDs
    df = download_ais(t_stamp = datetime.strptime(tstamp, in_format),  back_window = 0, forward_window = 24*number_of_days, ssvids=ssvids)
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))

    if len(df)>0:
        makedirs("/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/ssvid_search_ais/", exist_ok=True)
        if add_max_dev:
            stdev = gdf.groupby('ssvid').std()
            gdf["max_dev"] = stdev[["lon", "lat"]].max(axis=1)
        
        gdf.sort_values(by="timestamp", ascending=False).drop_duplicates(subset='ssvid').to_file(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/ssvid_search_ais/LAST_{filename}", driver="GeoJSON")
        if single_file:
            gdf.to_file(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/ssvid_search_ais/{filename}", driver="GeoJSON")
        else:
            for ssvid in gdf['ssvid'].unique().tolist():
                gdf[gdf['ssvid'] == ssvid].to_file(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/ssvid_search_ais/split_files/{str(ssvid)}.geojson", driver="GeoJSON")
    return gdf

def plot_ais_over_time(bulk_df, ssvids=None):
    # This function will plot the AIS broadcasts from multiple SSVIDs on a horizontal dot chart to show when they were or were not broadcasting
    assert "ssvid" in bulk_df.keys() and "timestamp" in bulk_df.keys()
    ssvids = bulk_df["ssvid"].unique() if ssvids is None else ssvids
    # Create the figure
    fig = go.Figure()
    for i, ssvid in enumerate(ssvids):
        df = bulk_df[bulk_df['ssvid']==ssvid]
        datelist = df["timestamp"].map(dateutil.parser.parse).to_list()
        fig.add_trace(go.Scatter(x=datelist, y=[-i] * len(datelist), mode="markers", marker_size=2, name=ssvid))
    fig.update_yaxes( showgrid=False, zeroline=False, zerolinecolor="black", zerolinewidth=1, showticklabels=False)
    fig.update_layout(height=200+20*len(ssvids), title=f"AIS Over Time")
    fig.show()

def segment_splitter(curve):
    return list(map(LineString, zip(curve.coords[:-1], curve.coords[1:])))

def z_linestring_dist(z_point, z_line_string):
    return np.min([z_lineseg_dist(z_point, seg) for seg in segment_splitter(z_line_string)])
    # XXX This loop is SLOW how to get rid of it?

def z_lineseg_dist(z_point, z_line_segment):
    # from https://stackoverflow.com/questions/54442057/
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

# p = Point(0,0,0)
# assert z_lineseg_dist(p, LineString(((0,0,0), (1,1,1)))) == 0
# assert z_lineseg_dist(p, LineString(((1,0,0), (1,1,0)))) == 1
# assert z_lineseg_dist(p, LineString(((1,1,0), (1,1,1)))) == np.sqrt(2)
# assert z_lineseg_dist(p, LineString(((1,0,0), (0,1,0)))) == 1/np.sqrt(2)
# assert z_lineseg_dist(p, LineString(((1,0,1), (0,1,1)))) == np.sqrt(3/2)
# assert z_lineseg_dist(p, LineString(((1,0,0), (0,1,1)))) == np.sqrt(2/3) # This one fails because of precision error

def rectangle_from_pid(pid, buff):
    geojson_path = vect_dir/(pid+".geojson")
    with open(str(geojson_path)) as f:
        g = shape(json.load(f))
    return g.minimum_rotated_rectangle.buffer(buff).minimum_rotated_rectangle

def disc_from_point(longitude, latitude, buff):
    g = shape(Point(longitude, latitude))
    return g.buffer(buff)

def sync_ais_from_point_time(longitude, latitude, tstamp, back_window=12, forward_window=1.5, buff=.5, spire_only=False):
    # tstamp must be a string in this format: "%Y%m%dT%H%M%S"
    ais_dir.mkdir(parents=True, exist_ok=True)    
    ais_path = ais_dir/(str(tstamp)+"_"+str(longitude)+"_"+str(latitude)+".geojson")
    if not ais_path.exists():
        disc = disc_from_point(longitude, latitude, buff)
        t_stamp = datetime.strptime(tstamp, in_format)
        df = download_ais(t_stamp, back_window, forward_window, spire_only, poly = str(disc))
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
        if len(df)>0:
            gdf.to_file(ais_path, driver="GeoJSON")
        else:
            print("No AIS data found for", ais_path.name)
    else:
        print("AIS already downloaded for", ais_path.name)    

def sync_ais_files(pids, back_window=12, forward_window=1.5, buff=.5, spire_only=False):
    ais_dir.mkdir(parents=True, exist_ok=True)
    for pid in pids:
        ais_path = ais_dir/(pid+".geojson")
        geojson_path = vect_dir/(pid+".geojson")

        if ais_path.exists():
            print(f"AIS already downloaded for {pid}")
            continue
        if not geojson_path.exists():
            print(f"No GeoJSON data found for {pid}")
            continue
        
        rect = rectangle_from_pid(pid, buff)
        time_from_pid = pid.split('_')[4]
        t_stamp = datetime.strptime(time_from_pid, in_format)
        df = download_ais(t_stamp, back_window, forward_window, spire_only, poly = str(rect))
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
        if len(df)>0:
            gdf.to_file(ais_path, driver="GeoJSON")
            gdf.to_csv(ais_path.with_suffix('.csv'), index=False)
        else:
            print("No AIS data found for", pid)
            

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
    # Find the point on the line segment between two 3D points [p1, p2] that has z value
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
        elif pd > distance:
            cp = line.interpolate(distance)
            return [
                LineString(coords[:i] + [(cp.x, cp.y, cp.z)]),
                LineString([(cp.x, cp.y, cp.z)] + coords[i:])]

def mae_ranking(pids, return_count=1, num_samples=50, vel=1):
    ## Buffer AIS linestrings to identify culprit
    # Set return_count equal to 0 to rank all vessels in file
    # vel is a ratio from time to distance, used as to add a Z dimension to the AIS points for coincidence scoring
    coincidence_dir.mkdir(parents=True, exist_ok=True)
    
    for pid in pids:
        print(f"Ranking AIS tracks for {pid}")
        vect_path = vect_dir/(pid+".geojson")
        ais_path = ais_dir/(pid+".geojson")
        coincidence_path = coincidence_dir/(pid+".csv")

        if coincidence_path.exists():
            # Check to see if the work was done previously
            coincidence_df = gpd.read_file(coincidence_path)
            if return_count <= len(coincidence_df):
                print(f"Using previous MAE ranking for {pid}")
                continue
        if not vect_path.exists():
            print(f"Could not find GeoJSON file for {pid}")
            continue
        if not ais_path.exists():
            print(f"Could not find AIS file for {pid}")
            continue

        # Open the slick multipolygon
        with open(vect_path) as f:
            geom = json.load(f)
        slick_shape = shape(geom).buffer(0)
        sample_points = sample_shape(slick_shape, num_samples)
        slick_samples_gs = gpd.GeoSeries([Point(*s, 0) for s in sample_points])

        # Open the AIS data for the same GRD
        ais_df = gpd.read_file(ais_path).sort_values('timestamp')

        # Add DeltaTime column
        capture_timestamp = datetime.strptime(pid.split("_")[4], in_format).replace(tzinfo=timezone.utc)
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

        # Calculate the Mean Absolute Error for each AIS Track, with early stopping if a candidate is looking bad.
        return_count = return_count or len(ssvid_df)
        ssvid_df['coinc_score'] = np.nan
        minimum_errors = [np.inf]*return_count
        for idx, vessel in tqdm(ssvid_df["ais_before_t0"].iteritems(), total=ssvid_df.shape[0]):
            if vessel: # XXX Note that "if vessel else None" means that we are ignoring vessels that only broadcast AIS AFTER the image was captured (this is not ideal)
                error_threshold = minimum_errors[-1]
                error_sum = 0
                for sample in slick_samples_gs:
                    error_sum += z_linestring_dist(sample, z_line_string=vessel)
                    running_error = error_sum/num_samples
                    if running_error > error_threshold: break
                else:
                    minimum_errors.append(running_error)
                    minimum_errors.sort()
                    minimum_errors.pop()
                    ssvid_df.at[idx,'coinc_score'] = running_error

        # # Suggest Abstention
        # abstain_threshold = 0.05 # This is a threshold value that determines how often we abstain from blaming a vessel in the picture (default = 0.01). Raise the value to make it more likely to blame a vessel.
        # ssvid_df = ssvid_df.append(pd.Series({'coinc_score':abstain_threshold},name="^^^ Abstain Above^^^"))
        rank = ssvid_df.sort_values('coinc_score')["coinc_score"].head(return_count)
        rank.to_csv(coincidence_path)

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
