""" This module stores the default config settings for the server.
It is optionally overriden by a local_config module in the same directory.
The local configs are for dev debugging/testing.
"""
import importlib

# Flask app settings
VERBOSE = False
CLEANUP_SNS = True
DOWNLOAD_GRDS = True
RUN_ML = True
UPDATE_ML = True
COUNTRIES_GEOJSON = "Countries.geojson"
EEZ_GEOJSON = "eez_v11_simplified_noholes.geojson"
OCEAN_GEOJSON = "OceanGeoJSON_lowres.geojson"
COUNTRIES_GEOJSON = "Countries.geojson"
EEZ_GEOJSON = "eez_v11_simplified_noholes.geojson"
BLOCK_REPEAT_SNS = True
SQS_URL = "https://sqs.eu-central-1.amazonaws.com/162277344632/New_Machinable"
UPLOAD_OUTPUTS = True
WKT_ROUNDING = 5 # -1 means don't round

# Database connection settings
DB_HOST = "db-slick.cboaxrzskot9.eu-central-1.rds.amazonaws.com"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_DATABASE = "cerulean"
DB_PORT = "5432"
DB_TYPE = "postgresql"
ECHO_SQL = False
# DEBUG_DB_ACCESS switches on/off all DB functionality if in debug mode
DEBUG_DB_ACCESS = True

# SciHub login settings
SH_USER = "jonaraph"
SH_PWD = "fjjEwvMDHyJH9Fa"

# If the local_config module is found, import all those settings, overriding any here that overlap.
if importlib.util.find_spec("configs.local_config") is not None:
    from configs.local_config import *  # pylint: disable=unused-wildcard-import
