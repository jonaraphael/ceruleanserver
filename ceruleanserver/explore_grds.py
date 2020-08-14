#%%
from utils.s3 import sync_grds_and_vecs


pids = [
    "S1B_IW_GRDH_1SDV_20200725T161522_20200725T161547_022630_02AF31_1521",
    ]

sync_grds_and_vecs(pids)
# %%

# Download all files
# cmd = f'aws s3 sync s3://skytruth-cerulean/outputs/ ../local/temp/outputs/'
# run(cmd, shell=True)
