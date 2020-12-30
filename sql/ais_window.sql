 SELECT DISTINCT ON (d.starttime)
    d.pid AS grd__pid,
	st_astext(d.geometry),
    d.starttime AS grd__starttime
FROM 
	posi_poly a
	JOIN inference c ON c.id = a.inference__id
	JOIN grd d ON d.id = c.grd__id
	JOIN slick e ON e.id = a.slick__id
 WHERE
--  	e.class_int = 1
	d.pid in (
		'S1B_IW_GRDH_1SDV_20201114T013822_20201114T013850_024254_02E1E1_4638'
		)
	

