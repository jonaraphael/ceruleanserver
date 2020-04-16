import psycopg2
import pandas as pd
import config

class DBConnection:
    def __init__(self, host=config.DB_HOST, user=config.DB_USER, password=config.DB_PASSWORD, 
        database=config.DB_DATABASE, port=config.DB_PORT):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
    
    def open(self):
        self.conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.database, port=self.port)
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()
    
    def close(self):
        self.cur.close()
        self.conn.close()
    
    def read_field_from_field_value_table(self, r, f, v, tbl):
        res = None
        cmd = f"""
            SELECT {r} FROM {tbl}
            WHERE {f} = {v}
        """
        # print(cmd)
        self.open()
        try:
            res = pd.read_sql_query(cmd, self.conn)
        except (pd.io.sql.DatabaseError) as e: # pylint: disable=no-member
            print('ERROR:', e)
        self.close()
        return res
    
    def insert_dict_as_row(self, dct, tbl):
        keys, values = zip(*dct.items())        
        cmd = f"""
            INSERT INTO {tbl}({', '.join(keys)})
            VALUES({', '.join([str(v) for v in values])})
        """
        # print(cmd)
        self.open()
        try:
            if config.DB_ACCESS:
                self.cur.execute(cmd)
            else:
                print("WARNING: Running on localhost. Would now write to table:", tbl)
        except psycopg2.errors.UniqueViolation as e: # pylint: disable=no-member
            print('ERROR:', e)
        self.close()