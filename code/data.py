import psycopg2
import config

class DBConnection:
    def __init__(self, host=config.DB_HOST, user=config.DB_USER, password=config.DB_PASSWORD, 
        database=config.DB_DATABASE, port=config.DB_PORT):
        self.conn = psycopg2.connect(host=host, user=user, password=password, database=database, port=port)
        self.conn.set_session(autocommit=True)
    
    def insert_dict_as_row(self, dct, tbl):
        keys, values = zip(*dct.items())        
        cur = self.conn.cursor()
        cmd = f"""
            INSERT INTO public."{tbl}"("{'", "'.join(keys)}")
            VALUES({', '.join([str(v) for v in values])})
        """
        # print(cmd)
        try:
            if config.DB_ACCESS:
                cur.execute(cmd)
            else:
                print("WARNING: Running on localhost. Would now write to table:", tbl)
        except psycopg2.errors.UniqueViolation as e: # pylint: disable=no-member
            print('ERROR:', e)
        cur.close()