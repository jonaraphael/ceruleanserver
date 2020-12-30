SELECT 
-- DISTINCT ON (grd__pid)
* 
FROM super_sans_eez
WHERE true
-- AND slick__class_int in (0,10,11,12,13,14,15,17) -- false positives
AND slick__class_int in (1,4,18,19) -- vessels
-- AND slick__class_int in (2,5,6,7,8,9) -- stationary
-- AND slick__class_int in (3,16) -- unknown
-- AND slick__class_int IS NULL
AND posi_poly__polsby_popper > 200
-- AND ARRAY_TO_STRING(eez__sovereigns,'||') ILIKE '%mauri%'
AND grd__starttime between '08/01/2020 00:00:00.000' and '09/01/2020 00:00:00.000'