""" 
Common utility functions. 
"""
from pathlib import Path
from datetime import datetime
import shapely.geometry as sh
import json
import sys

sys.path.append(str(Path(__file__).parent.parent))
from configs import path_config

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


def str_to_ts(s):
    """Turns a string into a timestamp
    
    Arguments:
        s {str} -- one of three formatted strings that occur in SNS or SciHub
    
    Returns:
        timestamp -- seconds since epoch
    """
    if "Z" in s:
        if "." in s:
            fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
        else:
            fmt = "%Y-%m-%dT%H:%M:%SZ"
    else:
        fmt = "%Y-%m-%dT%H:%M:%S"
    return datetime.strptime(s, fmt).timestamp()


def load_ocean_shape():
    """Read the ocean GeoJSON into memory once, so that it is accessible for all future functions

    Returns:
        [Geometry] -- A Shapely geometry produced from a GeoJSON
    """
    with open(path_config.LOCAL_DIR + "aux_files/OceanGeoJSON_lowres.geojson") as f:
        ocean_features = json.load(f)["features"]
    return sh.GeometryCollection(
        [sh.shape(feature["geometry"]).buffer(0) for feature in ocean_features]
    )[0]
