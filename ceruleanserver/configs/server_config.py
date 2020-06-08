""" This module stores the default config settings for the server.
It is optionally overriden by a local_config module in the same directory.
The local configs are for dev debugging/testing.
"""
import importlib

# Flask app settings
APP_HOST = "0.0.0.0"
APP_PORT = 5000
FLASK_DEBUG = False
VERBOSE = False
CLEANUP_SNS = True
DOWNLOAD_GRDS = True
RUN_ML = True
UPDATE_ML = True
OCEAN_GEOJSON = "OceanGeoJSON_lowres.geojson"
BLOCK_REPEAT_SNS = True
SQS_URL = "https://sqs.eu-central-1.amazonaws.com/162277344632/New_Machinable"

# Database connection settings
DB_HOST = "db-slick.cboaxrzskot9.eu-central-1.rds.amazonaws.com"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_DATABASE = "db_slick"
DB_PORT = "5432"
# DEBUG_DB_ACCESS switches on/off all DB functionality if in debug mode
DEBUG_DB_ACCESS = True

# SciHub login settings
SH_USER = "jonaraph"
SH_PWD = "fjjEwvMDHyJH9Fa"

# If the local_config module is found, import all those settings, overriding any here that overlap.
if importlib.util.find_spec("configs.local_config") is not None:
    from configs.local_config import *  # pylint: disable=unused-wildcard-import
