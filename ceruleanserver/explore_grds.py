#%%
%load_ext autoreload
%autoreload 2

pids = [
"S1A_IW_GRDH_1SDV_20210402T111750_20210402T111815_037271_046403_2F28",
"S1B_IW_GRDH_1SDV_20210720T033426_20210720T033451_027872_035360_F478"
]
print(len(pids), "GRDs provided")

# %%
from utils.s3 import sync_grds_and_vecs
sync_grds_and_vecs(pids, download_grds=False)

# %%
from ais_funcs import sync_ais_files
sync_ais_files(pids)

#%%
from ais_funcs import mae_ranking
mae_ranking(pids)

#%%
from ee_viewer import context_map
for p in pids: 
    context_map([p])
    input('Press Enter to display next PID group.')

# %%
# This code is used to grab a disc of AIS around a lon/lat/tstamp
from ais_funcs import sync_ais_from_point_time
sync_ais_from_point_time(49.055373, 27.623153, '20220719T145035') # Lon Lat

#%%
# This code is used to grab historic AIS tracks of a list of specific SSVIDs
from ais_funcs import bulk_download_ais
bulk_df = bulk_download_ais(filename="collision.geojson", tstamp = '20220815T000000', ssvids = [431014803,431014803], number_of_days = 7, add_max_dev=False)

#%%
# This code will plot the days that do or do not have AIS from each vessel in the bulk_df
from ais_funcs import plot_ais_over_time
# bulk_df = gpd.read_file(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/ssvid_search_ais/FPSOs_2022-07.geojson")
plot_ais_over_time(bulk_df)

#%%