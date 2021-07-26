# %%
# This code is used to clean up the heavily-duplicated infrastructure dataset from GFW. It does an OK job.
import pandas as pd
import geopandas as gpd
round_depth = 3
a = pd.read_csv("/Users/jonathanraphael/Desktop/Contracting Files/SkyTruth/detections_2020_infrastructureOnly_v20210604.csv")
a['droplist'] = (round(a['detect_lat']*(10**round_depth))).astype(str)+":"+(round(a['detect_lon']*(10**round_depth))).astype(str)
# b = a.drop_duplicates(['droplist','ssvid'])
# c = pd.concat([b[b['ssvid'].isnull()],b[b['ssvid'].notnull()].drop_duplicates('ssvid')])
# c.to_csv("~/Desktop/detections_2020_infrastructureOnly_v20210604_dedupe.csv")

b = a.drop_duplicates(['droplist'])
print(len(b))
b.to_csv("~/Desktop/detections_2020_infrastructureOnly_v20210604_dedupe.csv")
# then buffer by (.001), dissolve, split, and take the centroids in QGIS
# %%
# c = gpd.GeoDataFrame(b, geometry=gpd.points_from_xy(b.detect_lon, b.detect_lat))
# d = c
# d.geometry = c.buffer(.01)
# e = gpd.overlay(d, d, how='union')
# f = e.centroid()


# # %%
# import matplotlib.pyplot as plt
# fig, ax = plt.subplots(figsize = (10,6))
# ax.set_xlim([2.86,3.06])
# ax.set_ylim([51.59,51.69])
# d.plot(ax=ax)
# plt.show()
# print(".")

