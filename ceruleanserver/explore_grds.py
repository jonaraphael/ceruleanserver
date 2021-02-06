#%%
pids = [
    "S1B_IW_GRDH_1SDV_20210203T183427_20210203T183452_025446_0307F5_7A5E"
]

# %%
from utils.s3 import sync_grds_and_vecs
sync_grds_and_vecs(pids)

# %%
from ais_funcs import sync_ais_csvs, mae_ranking
sync_ais_csvs(pids)
mae_ranking(pids, 5)
# %%
