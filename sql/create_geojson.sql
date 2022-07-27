SELECT jsonb_build_object(
    'type',     'FeatureCollection',
    'features', jsonb_agg(feature)
)
FROM (
  SELECT jsonb_build_object(
    'type',       'Feature',
    'geometry',   ST_AsGeoJSON(geometry::geometry)::jsonb,
    'properties', to_jsonb(row) -'geometry'
  ) AS feature
  FROM (
	  SELECT pid, geometry
	  FROM grd 
	  WHERE starttime between '07/27/2020 00:00:00.000' and '07/27/2021 00:00:00.000'
	  LIMIT 10
  ) row) features;
