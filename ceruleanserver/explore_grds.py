#%%
from subprocess import run, PIPE
pids = [
    
] # up to 200 from Tues 2020-06-09 14:00

include_str = " ".join([f'--include "rasters/{pid}.tiff" --include "vectors/{pid}.geojson"' for pid in pids])
cmd = f'aws s3 sync s3://skytruth-cerulean/outputs/ ../local/temp/outputs/ --exclude "*" {include_str}'
run(cmd, shell=True)

# %%

# Download all files
# cmd = f'aws s3 sync s3://skytruth-cerulean/outputs/ ../local/temp/outputs/'
# run(cmd, shell=True)
