-- SELECT
-- grd__pid, grd__starttime, slick__class_int, posi_poly__polsby_popper, posi_poly__fill_factor, eez__geoname, eez__sovereigns, posi_poly__area, posi_poly__id, 
-- ST_AsEWKT(f.geom_dump::geography) AS wkt
-- FROM (
	
SELECT 
DISTINCT ON (grd__pid) 
*
-- , (ST_Dump(posi_poly__poly::geometry)).geom AS geom_dump

-- 	ST_AsGeoJSON(ST_collect(st_centroid(posi_poly__poly)::geometry)::geography, 6) as singlegeom
FROM superview
-- FROM eez_mapped
WHERE true
AND slick__class_int in (4,18,19,6)
-- AND slick__class_int in (2,6,7,8,9)
-- AND slick__class_int IS NULL
-- AND posi_poly__polsby_popper > 200
-- AND posi_poly__fill_factor > .5
-- AND ARRAY_TO_STRING(eez__sovereigns,'||') ILIKE '%guyan%'
-- AND eez__geoname ILIKE '%german%'
-- AND grd__starttime between '01/01/2021 00:00:00.000' and '01/01/2022 00:00:00.000' 
-- AND ST_Intersects(ST_GeomFromText(grd__geometry),ST_Buffer(ST_GeomFromText('POINT(-91.9988 19.3012)'), .1)) --POINT(LON.XXXX LAT.XXXX)
-- AND ST_Intersects(posi_poly__poly::geography,ST_Buffer(ST_GeomFromText('POINT(-91.9988 19.3012)'), .1)) --POINT(LON.XXXX LAT.XXXX)
-- AND ST_Intersects(posi_poly__poly,ST_GeomFromText('POLYGON ((-92 28, -90 28, -90 27, -92 27, -92 28))')) --POINT(LON.XXXX LAT.XXXX)
-- AND grd__pid in ()
-- GROUP BY True
ORDER BY grd__pid, posi_poly__polsby_popper DESC
-- ) as f