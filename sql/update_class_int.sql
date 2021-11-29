-- UPDATE slick SET class_int = 4 FROM
SELECT DISTINCT ON (grd.id) * FROM slick,

posi_poly, inference, grd
WHERE TRUE
	AND slick.id = posi_poly.slick__id
	AND inference.id = posi_poly.inference__id
	AND grd.id = inference.grd__id
-- 	AND grd.pid in (
-- 'S1B_IW_GRDH_1SDV_20210510T182511_20210510T182536_026846_0334FE_280B',
-- 		'' )
	AND slick.id in (
2000,8738,9428,199,12387,13396,15578,14559,5377,4280,828,6290
		)