#%%
pids = [
"S1A_IW_GRDH_1SDV_20210402T111750_20210402T111815_037271_046403_2F28",
"S1B_IW_GRDH_1SDV_20210720T033426_20210720T033451_027872_035360_F478"
]

# %%
from utils.s3 import sync_grds_and_vecs
sync_grds_and_vecs(pids)

# %%
from ais_funcs import sync_ais_files, mae_ranking
sync_ais_files(pids)
mae_ranking(pids, 5)

# %%
from ais_funcs import sync_ais_from_point_time
sync_ais_from_point_time(8.0726, 3.5563, "20210709T051422")

# %%
