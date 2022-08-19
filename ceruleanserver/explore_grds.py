#%%
pids = [
"S1A_IW_GRDH_1SDV_20210402T111750_20210402T111815_037271_046403_2F28",
"S1B_IW_GRDH_1SDV_20210720T033426_20210720T033451_027872_035360_F478"
]
print(len(pids), "GRDs provided")

# %%
from utils.s3 import sync_grds_and_vecs
sync_grds_and_vecs(pids, download_grds=False)

# %%
from ais_funcs import sync_ais_files, mae_ranking
sync_ais_files(pids)
# mae_ranking(pids, 5)

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
bulk_df = bulk_download_ais(tstamp = '20210101T000000', ssvids = [657198000,], number_of_days = 7, add_max_dev=True)

#%%
# This code will plot the days that do or do not have AIS from each vessel in the bulk_df
from ais_funcs import plot_ais_over_time
# bulk_df = gpd.read_file(f"/Users/jonathanraphael/git/ceruleanserver/local/temp/outputs/ssvid_search_ais/FPSOs_2022-07.geojson")
plot_ais_over_time(bulk_df)

#%%








# %%
# This code is used to parse the long-form outputs of Tatiana's IMO web scraper
import pandas as pd
fn = '/Users/jonathanraphael/Downloads/scraper_output_3.csv'
df = pd.read_csv(fn, encoding = 'unicode_escape', header=None)
column_names = ["Name", "IMO Number", "Flag", "Call sign", "MMSI", "Ship UN Sanction", "Owning/operating entity under UN Sanction", "Type", "Converted from", "Date of build", "Gross tonnage", "Registered owner"]
reshaped = pd.DataFrame(columns = column_names)

vessel_count = -1
for i, item in df[0].iteritems():
    idx = [key in item for key in column_names]
    if any(idx):
        col = column_names[idx.index(True)]
        if col == "Name":
            vessel_count+=1
        reshaped.at[vessel_count, col] = item[(len(col)+1):]        
reshaped.to_csv('/Users/jonathanraphael/Downloads/scraper_reshaped_3.csv')
# %%
