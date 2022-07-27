-- EXPLAIN (FORMAT JSON) 
SELECT 
-- DISTINCT ON (grd__pid) 
*
-- 	ST_AsGeoJSON(ST_collect(st_centroid(posi_poly__poly)::geometry)::geography, 6) as singlegeom
-- 	ST_AsGeoJSON(ST_collect(posi_poly__poly::geometry)::geography, 6) as singlegeom
FROM eez_mapped
WHERE true
AND slick__class_int = 0
-- AND slick__class_int in (2,6,7,8,9)
-- AND slick__class_int IS NULL
-- AND posi_poly__polsby_popper > 200
-- AND posi_poly__fill_factor > .25
-- AND ARRAY_TO_STRING(eez__sovereigns,'||') ILIKE '%trin%'
-- AND eez__geoname ILIKE '%german%'
-- AND grd__starttime between '01/01/2021 00:00:00.000' and '01/01/2022 00:00:00.000'
-- AND ST_Intersects(ST_GeomFromText(grd__geometry),ST_Buffer(ST_GeomFromText('POINT(-118.2064 33.6983)'), .1)) --POINT(LON.XXXX LAT.XXXX)
-- AND ST_Intersects(posi_poly__poly,ST_GeomFromText('POLYGON ((-92 28, -90 28, -90 27, -92 27, -92 28))')) --POINT(LON.XXXX LAT.XXXX)
-- AND grd__pid in ()
-- GROUP BY True
-- ORDER BY grd__pid, posi_poly__polsby_popper DESC




-- EXPLAIN (FORMAT JSON) 
SELECT
-- 	ST_AsGeoJSON(ST_collect(st_centroid(posi_poly__poly)::geometry)::geography, 6) as singlegeom
ST_AsGeoJSON(ST_collect(posi_poly__poly::geometry)::geography, 6) as singlegeom
FROM (
SELECT 
-- DISTINCT ON (grd__pid) 
*
FROM eez_mapped
WHERE true
-- AND slick__class_int = 0
AND slick__class_int in (19)
-- AND slick__class_int IS NULL
-- AND posi_poly__polsby_popper > 200
-- AND posi_poly__fill_factor > .25
-- AND ARRAY_TO_STRING(eez__sovereigns,'||') ILIKE '%trin%'
-- AND eez__geoname ILIKE '%german%'
-- AND grd__starttime between '01/01/2021 00:00:00.000' and '01/01/2022 00:00:00.000'
-- AND ST_Intersects(ST_GeomFromText(grd__geometry),ST_Buffer(ST_GeomFromText('POINT(-118.2064 33.6983)'), .1)) --POINT(LON.XXXX LAT.XXXX)
-- AND ST_Intersects(posi_poly__poly,ST_GeomFromText('POLYGON ((-92 28, -90 28, -90 27, -92 27, -92 28))')) --POINT(LON.XXXX LAT.XXXX)
-- AND grd__pid in ()
-- GROUP BY True
ORDER BY grd__pid, posi_poly__polsby_popper DESC
	) subselect