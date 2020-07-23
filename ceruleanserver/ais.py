# import geopandas
# from utils.common import load_shape
# from pathlib import Path
# import sys
# sys.path.append(str(Path(__file__).parent.parent))
# from configs import server_config
# # from shapely.geometry import shape
# from db_connection import DBConnection
# from alchemy import Eez
# import shapely.geometry as sh


# eez = load_shape(server_config.EEZ_GEOJSON)
# eez = geopandas.GeoDataFrame(eez)
# # eez['geometry'] = [shape(e) for e in eez['geometry']]

# db = DBConnection()  # Database Object

# for row in eez.itertuples():
#     sovs = [row.properties[sov] for sov in ['SOVEREIGN1', 'SOVEREIGN2', 'SOVEREIGN3'] if row.properties[sov] is not None]
#     geom = row.geometry
#     # geom = geom.update({"crs" : {"properties" : {"name" : "urn:ogc:def:crs:EPSG:8.8.1:4326"}}}) # This is equivalent to the existing projectionn, but is recognized by postgres as mappable, so slightly preferred.

#     e = Eez(
#         mrgid=int(row.properties['MRGID']),
#         geoname=row.properties['GEONAME'],
#         pol_type=row.properties['POL_TYPE'],
#         sovereigns=sovs,
#         geometry="SRID=4326;"+sh.shape(row.geometry).wkt
#     )
#     db.sess.add(e)
# db.sess.commit()
# db.sess.close()