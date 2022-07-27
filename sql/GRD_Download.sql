SELECT
ST_AsEWKT(f.geom_dump::geography) AS singlegeom
FROM (
	SELECT id, (ST_Dump(geometry::geometry)).geom AS geom_dump
	FROM grd
	WHERE starttime between '01/01/2021 00:00:00.000' and '01/01/2022 00:00:00.000' 
) AS f