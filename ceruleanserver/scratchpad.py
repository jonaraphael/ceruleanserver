# %%
# This code is used to clean up the heavily-duplicated infrastructure dataset from GFW. It does an OK job.
import pandas as pd
a = pd.read_csv("/Users/jonathanraphael/Desktop/Contracting Files/SkyTruth/detections_2020_infrastructureOnly_v20210604_dedupe.csv")
a['droplist'] = (round(a['detect_lat'],2)).astype(str)+":"+(round(a['detect_lon'],2)).astype(str)
b = a.drop_duplicates(['droplist','ssvid'])
c = pd.concat([b[b['ssvid'].isnull()],b[b['ssvid'].notnull()].drop_duplicates('ssvid')])
c.to_csv("~/Desktop/detections_2020_infrastructureOnly_v20210604_dedupe_2.csv")
