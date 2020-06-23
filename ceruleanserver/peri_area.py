#%%
from osgeo import ogr

pids = [
"S1B_IW_GRDH_1SDV_20200616T044634_20200616T044659_022054_029DA9_261D",
    ] # up to 297 from Tues 2020-06-09 14:00

for pid in pids:
    geojson_path = f"../local/temp/outputs/vectors/{pid}.geojson"
    a = ogr.Open(geojson_path)
    b = a.GetLayer()
    c = b.GetFeature(0)
    geom = c.geometry()
    print(f'{geom.GetArea():.10f},{geom.Boundary().Length():.10f}')

# %%
