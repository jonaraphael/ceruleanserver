""" This module stores the default config settings for the server.
It is optionally overriden by a local_config module in the same directory.
The local configs are for dev debugging/testing.
"""
from pathlib import Path
import importlib
import sys

# Flask app settings
APP_HOST = "0.0.0.0"
APP_PORT = 5000
DEBUG = False
VERBOSE = False

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

# ML Settings
MODEL_DWNLD_DIR = str(Path(__file__).parent.parent / 'temp' / 'model') # repo/ceruleanserver/temp/model
AWS_CLI = True
UPDATE_ML = True
RUN_ML = True
CLEANUP_SNS = True
CPU_ONLY = False

# If the local_config module is found, import all those settings, overriding any here that overlap.
sys.path.append(Path(__file__).parent)
if importlib.util.find_spec("configs.local_config") is not None:
    from configs.local_config import *  # pylint: disable=unused-wildcard-import
