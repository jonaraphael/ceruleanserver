import importlib

APP_HOST = '0.0.0.0'
APP_PORT = 80
DEBUG = True
    
DB_HOST = 'db-slick.cboaxrzskot9.eu-central-1.rds.amazonaws.com' 
DB_USER = 'postgres'
DB_PASSWORD = 'postgres' 
DB_DATABASE = 'db_slick' 
DB_PORT = '5432' 

SH_USER = 'jonaraph' 
SH_PWD = 'fjjEwvMDHyJH9Fa'

# override with values from a local file, only if it exists
if importlib.util.find_spec('local_config') is not None:
    from  local_config import *

