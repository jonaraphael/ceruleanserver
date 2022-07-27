#%%

import gdal
from datetime import datetime, timedelta
import pandas as pd
import geopandas
from shapely.geometry import MultiPolygon, LineString
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from configs import ais_config, server_config


class AISO:
    """A Class that correlates information between the AIS data and slick geometries
    """

    def __init__(self, grd_path, inf_path, grd_id=None, hours_before_image=ais_config.HOURS_BEFORE_IMAGE, hours_after_image=ais_config.HOURS_AFTER_IMAGE):
        # From SNS
        self.grd_path = grd_path
        self.inf_path = inf_path
        self.grd_id = grd_id or grd_path.parent.name

        # Calculated
        self.acq_time = datetime.strptime(self.grd_id[17:32], ais_config.GEOTIFF_TS_FORMAT)
        self.ts_max = self.acq_time + timedelta(hours=hours_after_image)
        self.ts_min = self.acq_time - timedelta(hours=hours_before_image)
        self.inf_multipolygon = geopandas.read_file(self.inf_path).geometry.iloc[0]

        # Placeholders
        self.coincidents = {}
        self.big_query_str = None
        self.ais_df = None
        self.lat_max = None
        self.lat_min = None
        self.lon_max = None
        self.lon_min = None
        self.ts_max = None
        self.ts_min = None

    def __repr__(self):
        return f"<AISObject: {self.grd_id}>"

    def get_big_query_str(self, buffer=ais_config.GRD_BUFFER_DEGREES):
        if not self.big_query_str:
            geotiff = gdal.Open(str(self.grd_path))
            GCPXs = [gcp.GCPX for gcp in geotiff.GetGCPs()]
            GCPYs = [gcp.GCPY for gcp in geotiff.GetGCPs()]
            del geotiff

            self.lat_max = max(GCPYs) + buffer
            self.lat_min = min(GCPYs) - buffer
            self.lon_max = max(GCPXs) + buffer
            self.lon_min = min(GCPXs) - buffer

            self.big_query_str = f"""
                SELECT
                    a.mmsi AS mmsi,
                    a.timestamp AS timestamp,
                    a.speed AS speed,
                    a.lat AS lat,
                    a.lon AS lon,
                    b.shipname AS shipname,
                    b.country_name AS country_name,
                    b.callsign AS callsign,
                    b.imo AS imo,
                    b.shiptype_text AS shiptype_text
                FROM (
                    SELECT
                        mmsi,
                        timestamp,
                        speed,
                        lat,
                        lon
                    FROM
                        [world-fishing-827:pipeline_p_p550_daily.classify_]
                    WHERE
                        lat > {self.lat_min} 
                        AND lat < {self.lat_max} 
                        AND lon > {self.lon_min} 
                        AND lon < {self.lon_max}
                        AND timestamp > TIMESTAMP('{self.ts_min.strftime(ais_config.BIGQUERY_TS_FORMAT)}')
                        AND timestamp < TIMESTAMP('{self.ts_max.strftime(ais_config.BIGQUERY_TS_FORMAT)}')
                ) a
                LEFT JOIN (
                    SELECT
                        shipname,
                        country_name,
                        callsign,
                        imo,
                        shiptype_text
                    FROM
                        [world-fishing-827:gfw_research_archived.vessel_info_20170717]
                    WHERE
                        year = 2016 
                ) b
                ON
                    a.mmsi = b.mmsi
            """
        return self.big_query_str
    
    def run_big_query(self):
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
        # https://cloud.google.com/bigquery/docs/running-jobs#bigquery_create_job-python
        self.ais_path = XXX_BIG_QUERY_API_CALL(self.get_big_query_str())
        
        df = pd.read_csv(self.ais_path, parse_dates=['timestamp']).sort_values('timestamp')
        self.ais_df = geopandas.GeoDataFrame(df,geometry=geopandas.points_from_xy(df.lon, df.lat))

    def find_coincidents(self):
        if not self.ais_df:
            self.run_big_query()
        
        self.coincidents = {}
        for poly_id, poly_geo in enumerate(self.inf_multipolygon): # For each polygon in the grd
            self.ais_df['dist'] = self.ais_df['geometry'].distance(poly_geo) # Add a new column that tracks distance from each point to the polygon
            self.ais_df['inv'] = 1 / self.ais_df['dist'] # Add another column that is the inverse of distance (weights close points more heavily)
            direct_hits = (self.ais_df['dist'] == 0) # Check if any AIS points fall inside polygon
            if not direct_hits.any(): # If there are none
                most_likely_mmsis = self.ais_df['mmsi'].unique() # All ships are potential culprits
            else: # If there are some direct hits
                hit_counts = self.ais_df[direct_hits].groupby(['mmsi']).count() # See how many times each MMSI hits
                most_likely_mmsis = hit_counts[hit_counts['dist'].values == hit_counts['dist'].max()].index # The ones that hit most are most likely
            
            proximity = self.ais_df[~direct_hits].groupby(['mmsi'])['inv'].mean() # Rate each MMSI by its average value in the inverse column, ignoring the direct hits (because their value is infinity)
            closest_mmsi = proximity[proximity.index.isin(most_likely_mmsis)].idxmax() # Select the MMSI that has the maximum score, only looking at the most likely vessels

            score = self.ais_df[self.ais_df['mmsi'].isin(most_likely_mmsis)].groupby(['mmsi'])['inv'].mean()[closest_mmsi] # Same, but include direct hits, to capture score
            
            coincidents.setdefault(closest_mmsi, []).append((poly_id, score)) # Record the poly as being associated with a certain mmsi
    
    def record_coincidents(self):
        if not self.coincidents:
            self.find_coincidents()
        for mmsi in self.coincidents:
            mmsi_multi = MultiPolygon([self.inf_multipolygon[poly_id[0]] for poly_id in self.coincidents[mmsi]]) # Combine polys that share a mmsi
            mmsi_string = LineString([p for p in self.ais_df[self.ais_df['mmsi']==mmsi]['geometry']]) # Turn this AIS track into a linestring for storage as well
            mmsi_info = self.ais_df[self.ais_df['mmsi']==mmsi].groupby(['mmsi']).agg(pd.Series.mode).loc[mmsi][['speed', 'receiver_type', 'receiver', 'status']] # Pull out the most common values from interesting columns for the culprit
            display(mmsi_multi)
            display(mmsi_string)
            display(mmsi_info)
            display(self.coincidents[mmsi])
            # Store in a database: [MMSI_info, Slick_Multipolygon, AIS_LineString, Coincidence_Score]

    
    def coincident_db_row(self):
        """Creates a dictionary that aligns with our coincident DB columns
        
        Returns:
            dict -- key for each column in our coincident DB, value from this slick and AIS content
        """
        tbl = "coincident"
        row = {
            "grd_id": f"'{self.grd_id}'",
            "inf_nam": f"'{self.inf_path.stem}'",
            "slick_multipoly": f"ST_GeomFromGeoJSON('{json.dumps(mmsi_multi)}')",
            "ais_linestring": f"ST_GeomFromGeoJSON('{json.dumps(mmsi_string)}')",
            "mmsi_info": f"'{mmsi_info}'",
        }
        return (row, tbl)


#%%
 
# import pandas as pd
# from pathlib import Path

# path = Path("/Users/jonathanraphael/git/ceruleanserver/local/temp/ais_data")
# fnames = [f for f in path.glob("*.csv") if f.is_file()]

# for fname in fnames:
#     print('reading', fname)
#     df = pd.read_csv(fname)

#     oils = [
#         [pd.Timestamp("20190809T090339Z"),-50.6,5.7,-47.9,8.1, "S1A_IW_GRDH_1SDV_20190809T090339_20190809T090408_028490_03386D_FC54"],
#         [pd.Timestamp("20190724T080108Z"),-36.7,-6.1,-34.0,-4.0, "S1A_IW_GRDH_1SDV_20190724T080108_20190724T080133_028256_033120_F0AB"],
#         [pd.Timestamp("20190719T075331Z"),-35.1,-8.3,-32.4,-6.2,"S1A_IW_GRDH_1SDV_20190719T075331_20190719T075356_028183_032EFB_15DB"],
#     ]
#     hours_back = pd.Timedelta(6,'hr')

#     for o in oils:
#         print("filtering", o[5])
#         mask = (
#             (df['lon'] > o[1]) &
#             (df['lat'] > o[2]) &
#             (df['lon'] < o[3]) &
#             (df['lat'] < o[4])
#         )
#         filt = df[mask]
#         filt['timestamp'] = pd.to_datetime(filt['timestamp'])
#         mask2 = (
#             (filt['timestamp'] < o[0]) &
#             (filt['timestamp'] > o[0] - hours_back)
#         )
#         filt2 = filt[mask2]

#         filt2.to_csv(path/f"{o[5]}_{fname.stem}.csv", index=False)

# %%

# from subprocess import run, PIPE
# cmd = 'aws s3 sync s3://bilge-dump-sample/ais_data/ ../local/temp/ais_data/'
# run(cmd, shell=True)

# %%

# import pandas as pd
# from pathlib import Path

# path = Path("/Users/jonathanraphael/git/ceruleanserver/local/temp/ais_data")
# fnames = [f for f in path.glob("S1A_IW_GRDH_1SDV_20190809T090339_20190809T090408_028490_03386D_FC54*.csv") if f.is_file()]

# combined_csv = pd.concat([pd.read_csv(f) for f in fnames ])
# combined_csv.to_csv( path/"S1A_IW_GRDH_1SDV_20190809T090339_20190809T090408_028490_03386D_FC54.csv", index=False, encoding='utf-8-sig')

#%%
https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert
https://cloud.google.com/bigquery/docs/running-jobs#bigquery_create_job-python

# SELECT
#   a.mmsi AS mmsi,
#   a.start AS start,
#   a.END AS END,
#   a.min_speed AS min_speed,
#   a.max_speed AS max_speed,
#   a.count AS count,
#   b.shipname AS shipname,
#   b.country_name AS country_name,
#   b.callsign AS callsign,
#   b.imo AS imo,
#   b.shiptype_text AS shiptype_text
# FROM (
#   SELECT
#     mmsi,
#     MIN(timestamp) AS start,
#     MAX(timestamp) AS END,
#     MIN(speed) AS min_speed,
#     MAX(speed) AS max_speed,
#     COUNT(*) AS count
#   FROM
#     TABLE_DATE_RANGE([world-fishing-827:pipeline_p_p550_daily.classify_], TIMESTAMP('2019-06-21'), TIMESTAMP('2019-06-21'))
#   WHERE
#     lat > -6.5187 - .1 and lat < -6.5187 + .1 and lon > 113.1082 -.1 and lon < 113.1082 +.1
# //    AND timestamp > TIMESTAMP('2017-01-08 08:00:00')
# //    AND timestamp < TIMESTAMP('2017-01-11 23:59:59')
#   GROUP BY
#     mmsi
#   ORDER BY
#     count DESC) a
# LEFT JOIN (
#   SELECT
#     *
#   FROM
#     [world-fishing-827:gfw_research_archived.vessel_info_20170717]
#   WHERE
#     year = 2016 ) b
# ON
#   a.mmsi = b.mmsi


# %%

# import gdal
# from pathlib import Path
# from datetime import datetime, timedelta

# fnames = [
#     "S1A_IW_GRDH_1SDV_20190809T090339_20190809T090408_028490_03386D_FC54",
#     "S1A_IW_GRDH_1SDV_20190719T075331_20190719T075356_028183_032EFB_15DB",
#     "S1A_IW_GRDH_1SDV_20190724T080108_20190724T080133_028256_033120_F0AB",
# ]
# res = []

# for fname in fnames:
#     fpath = Path(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/{fname}/vv_grd.tiff")
#     geotiff = gdal.Open(str(fpath))
#     GCPXs = [gcp.GCPX for gcp in geotiff.GetGCPs()]
#     GCPYs = [gcp.GCPY for gcp in geotiff.GetGCPs()]
#     del geotiff
#     spacerange = {
#         "lat_max" : max(GCPYs),
#         "lat_min" : min(GCPYs),
#         "lon_max" : max(GCPXs),
#         "lon_min" : min(GCPXs),
#     }

#     ts_in_format = '%Y%m%dT%H%M%S'
#     ts_out_format = '%Y-%m-%d %H:%M:%S'
#     acq_time = datetime.strptime(fname[17:32], ts_in_format)
#     window = timedelta(hours=6)
#     timerange = {
#         "ts_max" : acq_time.strftime(ts_out_format),
#         "ts_min" : (acq_time - window).strftime(ts_out_format)
#     }
#     print(fname)
#     print(spacerange)
#     print(timerange)
#     print('')

# %%

import geopandas
import pandas as pd
from pathlib import Path
from shapely.geometry import MultiPolygon, LineString

fnames = [
    "S1A_IW_GRDH_1SDV_20190809T090339_20190809T090408_028490_03386D_FC54",
    "S1A_IW_GRDH_1SDV_20190719T075331_20190719T075356_028183_032EFB_15DB",
    "S1A_IW_GRDH_1SDV_20190724T080108_20190724T080133_028256_033120_F0AB",
]

for fname in fnames:

    ais_path = Path(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/{fname}/{fname}.csv")
    df = pd.read_csv(ais_path, parse_dates=['timestamp']).sort_values('timestamp')
    ais = geopandas.GeoDataFrame(df,geometry=geopandas.points_from_xy(df.lon, df.lat))

    slick_path = Path(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/{fname}/slick_192-192-32-128conf.geojson")
    slick_multipoly = geopandas.read_file(slick_path).geometry.iloc[0]

    coincidents = {}
    for poly_id, poly_geo in enumerate(slick_multipoly): # For each polygon in the slick
        ais['dist'] = ais['geometry'].distance(poly_geo) # Add a new column that tracks distance from each point to the polygon
        ais['inv'] = 1 / ais['dist'] # Add another column that is the inverse of distance (weights close points more heavily)
        direct_hits = (ais['dist'] == 0) # Check if any AIS points fall inside polygon
        if not direct_hits.any(): # If there are none
            most_likely_mmsis = ais['mmsi'].unique() # All ships are potential culprits
        else: # If there are some direct hits
            hit_counts = ais[direct_hits].groupby(['mmsi']).count() # See how many times each MMSI hits
            most_likely_mmsis = hit_counts[hit_counts['dist'].values == hit_counts['dist'].max()].index # The ones that hit most are most likely
        
        proximity = ais[~direct_hits].groupby(['mmsi'])['inv'].mean() # Rate each MMSI by its average value in the inverse column, ignoring the direct hits (because their value is infinity)
        closest_mmsi = proximity[proximity.index.isin(most_likely_mmsis)].idxmax() # Select the MMSI that has the maximum score, only looking at the most likely vessels

        score = ais[ais['mmsi'].isin(most_likely_mmsis)].groupby(['mmsi'])['inv'].mean()[closest_mmsi] # Same, but include direct hits, to capture score
        
        coincidents.setdefault(closest_mmsi, []).append((poly_id, score)) # Record the poly as being associated with a certain mmsi

    for mmsi in coincidents:
        mmsi_multi = MultiPolygon([slick_multipoly[poly_id[0]] for poly_id in coincidents[mmsi]]) # Combine polys that share a mmsi
        mmsi_string = LineString([p for p in ais[ais['mmsi']==mmsi]['geometry']]) # Turn this AIS track into a linestring for storage as well
        # mmsi_info = ais[ais['mmsi']==mmsi].groupby(['mmsi']).agg(pd.Series.mode).loc[mmsi][['speed', 'receiver_type', 'receiver', 'status']] # Pull out the most common values from interesting columns for the culprit
        display(mmsi_multi)
        display(mmsi_string)
        # display(mmsi_info)
        display(coincidents[mmsi])
        # Store in a database: [MMSI_info, Slick_Multipolygon, AIS_LineString, Coincidence_Score]
    
# %%


import geopandas
import pandas as pd
from pathlib import Path
import json
from shapely.geometry import shape
from utils.common import load_shape
sys.path.append(str(Path(__file__).parent.parent))
from configs import ais_config, server_config


eezs = load_shape(server_config.EEZ_GEOJSON)
eezdf = geopandas.GeoDataFrame(eezs)
eezdf['geometry'] = [shape(c) for c in eezdf['geometry']]

#%%
fnames = [
    "S1A_IW_GRDH_1SDV_20190809T090339_20190809T090408_028490_03386D_FC54",
    "S1A_IW_GRDH_1SDV_20190719T075331_20190719T075356_028183_032EFB_15DB",
    "S1A_IW_GRDH_1SDV_20190724T080108_20190724T080133_028256_033120_F0AB",
]

for fname in fnames:
    slick_path = Path(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/{fname}/slick_192-192-32-128conf.geojson")
    slick_multipoly = geopandas.read_file(slick_path).geometry.iloc[0]
    if slick_multipoly:
        eezdf['dist'] = eezdf['geometry'].distance(slick_multipoly)
        closest = eezdf['dist'].idxmin()
        eez_name = eezdf['properties'].iloc[closest]['GEONAME']
        min_dist = round(eezdf['dist'].min()*100, 0)
        print(min_dist, "km from", eez_name)

# %%

# import geopandas
# from utils.common import load_shape, create_pg_array_string
# import json
# from pathlib import Path
# sys.path.append(str(Path(__file__).parent.parent))
# from configs import ais_config, server_config
# from shapely.geometry import shape
# from data import DBConnection


# eez = load_shape(server_config.EEZ_GEOJSON)
# eez = geopandas.GeoDataFrame(eez)
# # eez['geometry'] = [shape(e) for e in eez['geometry']]

# db = DBConnection()  # Database Object

# for row in eez.itertuples():
#     sovs = [row.properties[sov] for sov in ['SOVEREIGN1', 'SOVEREIGN2', 'SOVEREIGN3'] if row.properties[sov] is not None]
#     geom = row.geometry
#     # geom = geom.update({"crs" : {"properties" : {"name" : "urn:ogc:def:crs:EPSG:8.8.1:4326"}}}) # This is equivalent to the existing projectionn, but is recognized by postgres as mappable, so slightly preferred.

#     e = Eez(
#         mrgid=int(row.properties['MRGID']),
#         geoname=row.properties['GEONAME'],
#         pol_type=row.properties['POL_TYPE'],
#         sovereigns=sovs,
#         geometry="SRID=4326;"+sh.shape(row.geometry).wkt
#     )
#     db.sess.add(e)
# db.sess.commit()
# db.sess.close()
#%%

## Buffer AIS linestrings to identify culprit

# sys.path.append(str(Path(__file__).parent.parent))
# from configs import ais_config, server_config


from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
import json
from shapely.geometry import Point, LineString, shape, Polygon, MultiPolygon
from ml.vector_processing import geojson_to_shapely_multi

coinc_path = Path(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/vectors/")
for fpath in coinc_path.glob('*.geojson'):
    fstem = fpath.stem
    print(fstem)

    # Open the slick multipolygon
    polypath = coinc_path.parent/"vectors"/fpath
    with open(polypath) as f:
        geom = json.load(f)
    slick_shape = shape(geom).buffer(0)
    slick_gs = gpd.GeoSeries(slick_shape)

    # Open the AIS data for the same GRD
    ais_df = pd.read_csv(fstem+".csv").sort_values('timestamp')

    # Find any solitary AIS broadcasts and duplicate them, because linestrings can't exist with just one point
    singletons = ~ais_df.duplicated(subset='ssvid', keep=False)
    duped_df = ais_df.loc[np.repeat(ais_df.index.values,singletons+1)]

    # Zip the coordinates into a point object and convert to a GeoData Frame
    geometry = [Point(xy) for xy in zip(duped_df.lon, duped_df.lat)]
    geo_df = gpd.GeoDataFrame(duped_df, geometry=geometry)

    # Create a new GDF that uses the SSVID to create linestrings
    ssvid_df = geo_df.groupby(['ssvid'])['geometry'].apply(lambda x: LineString(x.tolist()))
    ssvid_df = gpd.GeoDataFrame(ssvid_df, geometry='geometry')

    # Initialize some search parameters
    ssvid_df['coinc_score'] = 0
    buffer_incr = 0.1
    buffered_series = ssvid_df['geometry']
    ssvid_df['max_buffered'] = None

    print("Beginning First Buffer")
    while True:

        # Buffer the AIS tracks
        buffered_series = buffered_series.buffer(buffer_incr)
        # Calculate Slick Intersection as % (between 0 and 1)
        coverage_percent = buffered_series.intersection(slick_gs[0]).area/slick_gs[0].area
        # Create the integrand (between 0 and buffer_incr)
        incr_score = (1 - coverage_percent) * buffer_incr
        ssvid_df['coinc_score'].update(ssvid_df['coinc_score'] + incr_score)

        # Treat AIS buffers that completely cover the slick
        completed = buffered_series[coverage_percent==1]
        ssvid_df['max_buffered'].update(completed)
        buffered_series.drop(completed.index, inplace=True)

        print(len(coverage_percent), max(coverage_percent))

        if min(coverage_percent) == 1:
            # Plot the max_buffered geometry, the slick, and the AIS tracks
            sorted_score = gpd.GeoDataFrame(ssvid_df.sort_values('coinc_score', ascending=False), geometry="max_buffered")
            ax = sorted_score.plot(cmap = "YlOrRd", alpha=0.3, figsize=(10,10))
            slick_gs.plot(ax=ax, color='black')
            ssvid_df.sort_values('coinc_score', ascending=False).plot(ax=ax, cmap = "Blues")

            # Print the final score (closest to 0 is best)
            print(sorted_score['coinc_score'])
            break



#%%
from datetime import datetime, timedelta
import pandas_gbq
from pathlib import Path
from configs import path_config

# Load from config
project_id = "world-fishing-827"
in_format = "%Y%m%dT%H%M%S"
d_format = "%Y-%m-%d"
t_format = "%Y-%m-%d %H:%M:%S"


def download_ais(pid, poly=poly, back_window=12, forward_window=1.5):
    time_from_pid = pid.split('_')[4]
    t_stamp = datetime.strptime(time_from_pid, in_format)

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

def rectangle_from_pid(pid, buff=.3):
    geojson_path = fdir/"vectors"/(pid+".geojson")
    with open(str(geojson_path)) as f:
        g = shape(geojson.load(f))
    return g.minimum_rotated_rectangle.buffer(buff).minimum_rotated_rectangle


import json
import geojson
from shapely.geometry import shape

fdir = Path(path_config.LOCAL_DIR)/"temp/outputs/"
fdir.mkdir(parents=True, exist_ok=True)

targets = [
    "S1B_IW_GRDH_1SDV_20200909T034850_20200909T034915_023293_02C3CD_F327",
]
for pid in targets:
    ais_path = fdir/"ais"/(pid+".csv")
    if not ais_path.exists():
        rect = rectangle_from_pid(pid)
        df = download_ais(pid, poly=str(rect))
        df.to_csv(ais_path)

# %%
