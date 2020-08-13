from pathlib import Path
import sys
import shapely.geometry as sh
from shapely import wkt, wkb
from shapely.ops import unary_union
import json
from geoalchemy2.elements import WKBElement, WKTElement

sys.path.append(str(Path(__file__).parent.parent))
from configs import server_config


def geojson_to_shapely_multi(geojson_path):
    with open(geojson_path) as f:
        features = json.load(f)["features"]
    polys = [sh.shape(feat["geometry"]).buffer(0) for feat in features]
    multi = unary_union(polys)
    if isinstance(multi, sh.Polygon):
        multi = sh.MultiPolygon([multi])

    return multi


def unify(json_paths):
    """Merge multiple GeoJsons with overlapping polygons into a single GeoJSON with "union" applied to all overlapping features

    Arguments:
        json_paths {list of Paths} -- Multi-featured GeoJSONs to be merged

    Returns:
        MultiPolygon -- shapely multipolygon containing all the polygons from all the geojsons dissolved together
    """
    if server_config.VERBOSE:
        print("Combining all model outputs")

    polys = []
    for path in json_paths:
        with open(path) as f:
            features = json.load(f)["features"]
        polys += [sh.shape(feat["geometry"]).buffer(0) for feat in features if sh.shape(feat["geometry"]).geom_type in ["Polygon", "Multipolygon"]] # Ignore points and lines
    uni = unary_union(polys)
    if isinstance(uni, sh.Polygon):
        uni = sh.MultiPolygon([uni])

    return uni


def sparsify(union, subset_paths):
    """Helper function to discard polygons from a superset that do not intersect at least one geometry from each subset geojson

    Arguments:
        union {MultiPolygon} -- superset Multipolygon that overlaps the subset GeoJSONs
        subset_paths {List of Paths} -- List of Locations of the small disparate GeoJSONs

    Returns:
        MultiPolygon -- thinned out version of union
    """
    # Function to delete features that don't intersect another file
    if server_config.VERBOSE:
        print("Discarding extraneous polygons")

    remove_polys = []
    for path in subset_paths:
        sub_multi = geojson_to_shapely_multi(path)
        for i, super_poly in enumerate(union):
            if not super_poly.intersects(sub_multi):
                remove_polys += [i]

    thinned_polys = []
    for i, super_poly in enumerate(union):
        if not i in remove_polys:
            thinned_polys += [super_poly]

    sparse = unary_union([sh.shape(p) for p in thinned_polys])
    if isinstance(sparse, sh.Polygon):
        sparse = sh.MultiPolygon([sparse])

    return sparse


def intersection(geojson_path, multi_to_trim):
    """Function to clip the intersection between two shapes

    Arguments:
        geojson_path {Path} -- Location of one of the GeoJSON
        multi_to_trim {MultiPolygon} -- a shapely object to trim

    Returns:
        Path -- The location of the output GeoJSON
    """
    if server_config.VERBOSE:
        print("Trimming to finest resolution")

    multi = geojson_to_shapely_multi(geojson_path)
    inter = multi.intersection(multi_to_trim)
    if isinstance(inter, sh.Polygon):
        inter = sh.MultiPolygon([inter])
    
    return inter


def shapely_to_geojson(shape, out_path):
    with open(str(out_path), "w") as outfile:
        json.dump(sh.mapping(shape), outfile)


def geojson_to_ewkt(geojson, srid="4326"):
    """Turn a geojson object into a string that geoalchemy2 will accept for the database

    Args:
        geojson (geojson): The GeoJSON that needs to be inserted
        srid (str, optional): The SRID. Defaults to "4326".

    Returns:
        str: A database-friendly text version
    """
    return f"SRID={srid};{wkt.dumps(sh.shape(geojson), rounding_precision=server_config.WKT_ROUNDING)}"


def shape_to_ewkt(shapely_shape, srid="4326"):
    """Turn a geojson object into a string that geoalchemy2 will accept for the database

    Args:
        shapely_shape (shape): The shape that needs to be inserted
        srid (str, optional): The SRID. Defaults to "4326".

    Returns:
        str: A database-friendly text version
    """
    return f"SRID={srid};{wkt.dumps(shapely_shape, rounding_precision=server_config.WKT_ROUNDING)}"

