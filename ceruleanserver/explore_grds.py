#%%
pids = [
"S1A_IW_GRDH_1SDV_20210402T111750_20210402T111815_037271_046403_2F28",
"S1B_IW_GRDH_1SDV_20210720T033426_20210720T033451_027872_035360_F478"
]
print(len(pids), "GRDs provided")

# %%
from utils.s3 import sync_grds_and_vecs
sync_grds_and_vecs(pids, download_grds=False)

#%%
from ee_viewer import context_map
for p in pids: 
    context_map([p])
    input('Press Enter to display next PID group.')

# %%
from ais_funcs import sync_ais_files, mae_ranking
sync_ais_files(pids)
# mae_ranking(pids, 5)

# %%
from ais_funcs import sync_ais_from_point_time
sync_ais_from_point_time(49.055373, 27.623153, '20220719T145035') # Lon Lat

#%%
from ais_funcs import download_ais, in_format
from datetime import datetime
import geopandas as gpd

df = download_ais(t_stamp = datetime.strptime('20210101T000000', in_format),  back_window = 0, forward_window = 24*364, ssvids=[657198000])
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
if len(df)>0:
    gdf.to_file("/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/ais/custom_ais_download.geojson", driver="GeoJSON")

#%%

# %%
