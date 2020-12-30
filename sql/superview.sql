SELECT 
DISTINCT ON (grd__pid)
* 
-- ST_AsGeoJSON(ST_collect(st_centroid(posi_poly__poly)::geometry)::geography, 6) as singlegeom
FROM superview
WHERE true
-- AND slick__class_int in (1,2,3)
AND slick__class_int IS NULL
AND posi_poly__polsby_popper > 200
-- AND posi_poly__linear_popper > 100
-- AND ARRAY_TO_STRING(eez__sovereigns,'||') ILIKE '%mauri%'
AND grd__starttime between '01/01/2020 00:00:00.000' and '01/01/2021 00:00:00.000'