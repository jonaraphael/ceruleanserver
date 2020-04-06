from local_config import Local_Config
class Config:
    app_host = '0.0.0.0' if not hasattr(Local_Config, 'app_host') else Local_Config.app_host
    app_port = 80 if not hasattr(Local_Config, 'app_port') else Local_Config.app_port
    app_debug = True if not hasattr(Local_Config, 'app_debug') else Local_Config.app_debug
    
    db_host = 'slick-db.cboaxrzskot9.eu-central-1.rds.amazonaws.com' if not hasattr(Local_Config, 'db_host') else Local_Config.db_host
    db_user = 'postgres' if not hasattr(Local_Config, 'db_user') else Local_Config.db_user
    db_password = 'postgres' if not hasattr(Local_Config, 'db_password') else Local_Config.db_password
    db_database = 'slick_db' if not hasattr(Local_Config, 'db_database') else Local_Config.db_database
    db_port = '5432' if not hasattr(Local_Config, 'db_port') else Local_Config.db_port

    sh_user = 'jonaraph' if not hasattr(Local_Config, 'sh_user') else Local_Config.sh_user
    sh_pwd = 'fjjEwvMDHyJH9Fa' if not hasattr(Local_Config, 'sh_pwd') else Local_Config.sh_pwd
print(Config.__dict__)