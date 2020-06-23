#%%
from subprocess import run, PIPE
pids = [
"S1B_IW_GRDH_1SDV_20200611T221609_20200611T221638_021992_029BD4_6EB2",
"S1A_IW_GRDH_1SDV_20200614T133459_20200614T133528_033014_03D2F0_82E9",
"S1A_IW_GRDH_1SDV_20200613T214837_20200613T214902_033004_03D2A8_8DF5",
"S1A_IW_GRDH_1SDV_20200612T173753_20200612T173812_032987_03D228_23BF",
"S1A_IW_GRDH_1SDV_20200611T004122_20200611T004140_032962_03D16E_5A11",
"S1A_IW_GRDH_1SDV_20200610T221744_20200610T221813_032961_03D165_10EA",
    ] # up to 297 from Tues 2020-06-09 14:00

cmd = 'aws s3 sync s3://skytruth-cerulean/outputs/ ../local/temp/outputs/ --exclude "*" '

include_tiffs = " ".join([f'--include "rasters/{pid}.tiff" ' for pid in pids])
cmd = cmd + include_tiffs

include_geos = " ".join([f'--include "vectors/{pid}.geojson" ' for pid in pids])
cmd = cmd + include_geos

run(cmd, shell=True)

# %%

# Download all files
# cmd = f'aws s3 sync s3://skytruth-cerulean/outputs/ ../local/temp/outputs/'
# run(cmd, shell=True)
