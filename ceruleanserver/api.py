#%%
from data_objects import (
    Slick_Ext,
    Posi_Poly_Ext,
    Inference_Ext,
    Grd_Ext,
    Eez_Ext,
    Coincident_Ext,
    Vessel_Ext,
)
from sqlalchemy import func, or_
from db_connection import session_scope

# All lists are ORed together
# All key/values are ANDed together

MockRequest = {
    # "aoi" : "SRID=4326; POLYGON ((149 -9, 148 -9, 148 -8, 149 -8, 149 -9))",
    "startdate" : "2021-01-01",
    "enddate" : "2021-02-01",
    # "eez_sov" : ["australia", "papua"],
    "limit" : 5,
    # "offset" : 5,
    # "count" : True,

    ### NOT IMPLEMENTED YET ###
    # "min_score" : 50,
    # "vessel_mmsi" : [1234, 5678],
    # "vessel_flag" : ["usa", "panama"],
    # "vessel_type" : ["tanker", "cruise"],
    # "drop_columns" : ["id", "geometry"],
    # "group_by" : "sov"
}

print(MockRequest)
db = "cerulean"

with session_scope(commit=False, database=db) as sess:
    q = sess.query(Slick_Ext)

    # Order by time:
    q = q.unique_join(Posi_Poly_Ext).unique_join(Inference_Ext).unique_join(Grd_Ext)
    q = q.order_by(Grd_Ext.starttime)

    if MockRequest.get("startdate"):
        q = q.filter(Grd_Ext.starttime >= MockRequest.get("startdate"))
    if MockRequest.get("enddate"):
        q = q.filter(Grd_Ext.starttime <= MockRequest.get("enddate"))
    if MockRequest.get("eez_sov"):
        q = q.unique_join(Posi_Poly_Ext).unique_join(
            Eez_Ext, func.ST_Intersects(Posi_Poly_Ext.geometry, Eez_Ext.geometry)
        )
        q = q.filter(
            or_(
                (
                    func.array_to_string(Eez_Ext.sovereigns, "||").ilike(f"%{sov}%")
                    for sov in MockRequest.get("eez_sov")
                )
            )
        )
    if MockRequest.get("min_score"):
        q = q.unique_join(Posi_Poly_Ext).unique_join(Coincident_Ext)
        q = q.filter(Coincident_Ext.score >= MockRequest.get("min_score"))
    if MockRequest.get("vessel_mmsi"):
        q = (
            q.unique_join(Posi_Poly_Ext)
            .unique_join(Coincident_Ext)
            .unique_join(Vessel_Ext)
        )
        q = q.filter(
            or_((Vessel_Ext.mmsi == m for m in MockRequest.get("vessel_mmsi")))
        )
    if MockRequest.get("vessel_flag"):
        q = (
            q.unique_join(Posi_Poly_Ext)
            .unique_join(Coincident_Ext)
            .unique_join(Vessel_Ext)
        )
        q = q.filter(
            or_((Vessel_Ext.flag == m for m in MockRequest.get("vessel_flag")))
        )
    if MockRequest.get("vessel_type"):
        q = (
            q.unique_join(Posi_Poly_Ext)
            .unique_join(Coincident_Ext)
            .unique_join(Vessel_Ext)
        )
        q = q.filter(
            or_((Vessel_Ext.shiptype == m for m in MockRequest.get("vessel_type")))
        )
    if MockRequest.get("aoi"):
        q = q.filter(func.ST_Intersects(Posi_Poly_Ext.geometry, MockRequest.get("aoi")))
    if MockRequest.get("limit"):
        if MockRequest.get("offset"):
            q = q.offset(MockRequest.get("offset"))
        q = q.limit(MockRequest.get("limit")).distinct()

    if MockRequest.get("count"):
        print(q.distinct().count())
    else:
        print([s.to_api_dict(sess) for s in q.all()])

# %%
