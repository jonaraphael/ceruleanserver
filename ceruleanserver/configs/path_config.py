import importlib

# Path Settings
LOCAL_DIR = "/home/ubuntu/ceruleanserver/local/"

# If the local_config module is found, import all those settings, overriding any here that overlap.
if importlib.util.find_spec("configs.local_config") is not None:
    from configs.local_config import *  # pylint: disable=unused-wildcard-import
