""" This module stores the default config settings for the server.
It is optionally overriden by a local_config module in the same directory.
The local configs are for dev debugging/testing.
"""
import importlib

#
HOURS_BEFORE_IMAGE = 6
HOURS_AFTER_IMAGE = 0
GEOTIFF_TS_FORMAT = "%Y%m%dT%H%M%S"
BIGQUERY_TS_FORMAT = "%Y-%m-%d %H:%M:%S"
GRD_BUFFER_DEGREES = 0

# If the local_config module is found, import all those settings, overriding any here that overlap.
if importlib.util.find_spec("configs.local_config") is not None:
    from configs.local_config import *  # pylint: disable=unused-wildcard-import
