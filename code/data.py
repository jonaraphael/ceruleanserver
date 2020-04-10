import psycopg2
import config

class DBConnection:
    def __init__(self, host=config.DB_HOST, user=config.DB_USER, password=config.DB_PASSWORD, 
        database=config.DB_DATABASE, port=config.DB_PORT):
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

