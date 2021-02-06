SELECT 
DISTINCT ON (grd__pid) 
*
-- 	ST_AsGeoJSON(ST_collect(st_centroid(posi_poly__poly)::geometry)::geography, 6) as singlegeom
-- 	ST_AsGeoJSON(ST_collect(posi_poly__poly::geometry)::geography, 6) as singlegeom
FROM superview
WHERE true
-- AND slick__class_int in (19)
-- AND slick__class_int in (2,6,7,8,9)
AND slick__class_int IS NULL
AND posi_poly__polsby_popper > 200
-- AND posi_poly__fill_factor > .5
-- AND ARRAY_TO_STRING(eez__sovereigns,'||') ILIKE '%indones%'
-- AND grd__starttime between '01/01/2021 00:00:00.000' and '01/01/2022 00:00:00.000'
-- AND ST_Intersects(ST_GeomFromText(grd__geometry),ST_Buffer(ST_GeomFromText('POINT(-118.2064 33.6983)'), .1)) --POINT(LON.XXXX LAT.XXXX)
-- AND grd__pid in ()
-- GROUP BY True