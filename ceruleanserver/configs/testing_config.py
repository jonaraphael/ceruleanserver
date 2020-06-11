import importlib

TESTDATA_FOLDER = "test_fixtures/"
TESTDATA_VERSION = "v0.0.0"

# If the local_config module is found, import all those settings, overriding any here that overlap.
if importlib.util.find_spec("configs.local_config") is not None:
    from configs.local_config import *  # pylint: disable=unused-wildcard-import
