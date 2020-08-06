""" 
Common utility functions. 
"""
from pathlib import Path
from datetime import datetime, timezone
import shapely.geometry as sh
from subprocess import run, PIPE
import json
import sys

sys.path.append(str(Path(__file__).parent.parent))
from configs import path_config, server_config


def clear(p):
    """Delete file if it exists

    Arguments:
        p {Path} -- file to be deleted
    """
    # This function can be replced by Path.unlink.(missing_ok=True) when we upgrade python to 3.8
    if p.exists():
        p.unlink()


def xml_get(lst, a, key1="@name", key2="#text"):
    """Extract a field from parsed XML
    
    Arguments:
        lst {list} -- a list of elements all sharing the same data type (e.g. str)
        a {str} -- the name of an XML tag you want
    
    Keyword Arguments:
        key1 {str} -- the field where a is stored (default: {"@name"})
        key2 {str} -- the type of data that a is (default: {"#text"})
    
    Returns:
        any -- the value of the XML tag that has the name 'a'
    """
    # from a lst of dcts, find the dct that has key value pair (@name:a), then retrieve the value of (#text:?)
    if lst == None:
        return None  # This is a hack for the case where there is no OCN product. TODO handle absent OCN higher up
    for dct in lst:
        if dct.get(key1) == a:
            return dct.get(key2)
    return None


def load_shape(geom_name, as_multipolygon=False):
    """Read a GeoJSON into memory once, so that it is accessible for all future functions

    Returns:
        [Geometry] -- A Shapely geometry produced from a GeoJSON
    """
    geom_path = Path(path_config.LOCAL_DIR) / "aux_files" / geom_name
    if server_config.VERBOSE:
        print("Loading GeoJSON:", geom_name)
    print()
    if not geom_path.exists():  # pylint: disable=no-member
        src_path = "s3://skytruth-cerulean/aux_files/" + geom_name
        download_str = f"aws s3 cp {src_path} {geom_path}"
        # print(download_str)
        run(download_str, shell=True)

    with open(geom_path, encoding='utf-8') as f:
        geom = json.load(f)["features"]
    if as_multipolygon:
        geom = sh.GeometryCollection(
            [sh.shape(feature["geometry"]).buffer(0) for feature in geom]
        )[0]
    return geom

def load_shape(geom_name, as_multipolygon=False):
    """Read a GeoJSON into memory once, so that it is accessible for all future functions

    Returns:
        [Geometry] -- A Shapely geometry produced from a GeoJSON
    """
    geom_path = Path(path_config.LOCAL_DIR) / "aux_files" / geom_name
    if server_config.VERBOSE:
        print("Loading GeoJSON:", geom_name)
    print()
    if not geom_path.exists():  # pylint: disable=no-member
        src_path = "s3://skytruth-cerulean/aux_files/" + geom_name
        download_str = f"aws s3 cp {src_path} {geom_path}"
        # print(download_str)
        run(download_str, shell=True)
 
    with open(geom_path) as f:
        geom = json.load(f)["features"]
    if as_multipolygon:
        geom = sh.GeometryCollection(
            [sh.shape(feature["geometry"]).buffer(0) for feature in geom]
        )[0]
    return geom


def create_pg_array_string(lst):
    if isinstance(lst[0], str):
        out = '{"' + '","'.join(lst) + '"}'
    elif isinstance(lst[0], (int, float)):
        out = "{" + ",".join([str(l) for l in lst]) + "}"
    else:
        print("ERROR -- Unkown type being turned into string")
    return out

def to_standard_datetime_str(dt):
    datetime_str = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return datetime_str
