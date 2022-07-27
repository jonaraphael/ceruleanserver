 SELECT a.geometry::geometry(Polygon,4326) AS posi_poly__poly,
    st_astext(d.geometry) AS grd__geometry,
    d.pid AS grd__pid,
    d.starttime AS grd__starttime,
    e.class_int AS slick__class_int,
    e.id AS slick__id,
    a.polsby_popper AS posi_poly__polsby_popper,
    a.fill_factor AS posi_poly__fill_factor,
    b.geoname AS eez__geoname,
    b.sovereigns AS eez__sovereigns,
    st_x(a.centroid::geometry) AS posi_poly__longitude,
    st_y(a.centroid::geometry) AS posi_poly__latitude,
    a.area AS posi_poly__area,
    a.id AS posi_poly__id
   FROM posi_poly a
      LEFT JOIN map_posi_poly_TO_eez map ON map.posi_poly__id = a.id
      LEFT JOIN eez b ON b.id = map.eez__id
      JOIN inference c ON c.id = a.inference__id
      JOIN grd d ON d.id = c.grd__id
      LEFT JOIN slick e ON e.id = a.slick__id
  ORDER BY (a.pp_ff) DESC;