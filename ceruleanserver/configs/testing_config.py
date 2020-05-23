import importlib
S3_TESTDATA_VERSION = 'v0.0.0'
S3_TESTDATA_PATH = f'test_fixtures/{S3_TESTDATA_VERSION}/'
LOCAL_TESTDATA_PATH = 'test_fixtures'

# If the local_config module is found, import all those settings, overriding any here that overlap.

if importlib.util.find_spec("local_config") is not None:
    from local_config import *  # pylint: disable=unused-wildcard-import