import importlib

# Path Settings
LOCAL_DIR = "/Users/jonathanraphael/git/skytruth/local/"

# If the local_config module is found, import all those settings, overriding any here that overlap.
if importlib.util.find_spec("local_config") is not None:
    from local_config import *  # pylint: disable=unused-wildcard-import
