import psycopg2
from config import Config

class DBConnection:
    def __init__(self, host=Config.db_host, user=Config.db_user, password=Config.db_password, database=Config.db_database, port=Config.db_port):
        self.conn = psycopg2.connect(host=host, user=user, password=password, database=database, port=port)
        self.conn.set_session(autocommit=True)
        
    def insert_into_image_table(self, sns_msg, oceanic):
        cur = self.conn.cursor()
        cmd = f"""
            INSERT INTO public."IMAGE"
            VALUES(
                '{sns_msg["id"]}',
                '{sns_msg["startTime"].replace("T", " ")}',
                '{oceanic}',
                ST_GeomFromGeoJSON('{json.dumps(sns_msg["footprint"])}'),
                '{sns_msg["polarization"]}',
                '{sns_msg["mode"]}',
                '{sns_msg["path"]}'
            )
        """
        cur.execute(cmd)
        cur.close()

