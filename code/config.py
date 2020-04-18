import importlib

APP_HOST = '0.0.0.0'
APP_PORT = 5000
DEBUG = True

DB_HOST = 'db-slick.cboaxrzskot9.eu-central-1.rds.amazonaws.com' 
DB_USER = 'postgres'
DB_PASSWORD = 'postgres' 
DB_DATABASE = 'db_slick' 
DB_PORT = '5432' 
DEBUG_DB_ACCESS = True # switches on/off all DB functionality if in debug mode

SH_USER = 'jonaraph' 
SH_PWD = 'fjjEwvMDHyJH9Fa'

ML_PKL = ''
RUN_ML = False
CLEANUP_SNS = True

# override with values from a local file, only if it exists
if importlib.util.find_spec('local_config') is not None:
    from  local_config import * # pylint: disable=unused-wildcard-import