import psycopg2
class DBConnection:
    def init(self, host, user, password, database, port):
        self.conn = psycopg2.connect(host, user, password, database, port)
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

