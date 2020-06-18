-- GRANT EXECUTE ON FUNCTION list_intersecting TO anon;

CREATE OR REPLACE FUNCTION public.list_intersecting(featurecollection json) RETURNS SETOF inference AS $$

SELECT * FROM inference 
WHERE ST_Intersects(
	ST_GeomFromGeoJSON(featurecollection->'features'->0->'geometry'),
	geometry);

$$ LANGUAGE SQL;