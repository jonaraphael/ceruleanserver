import psycopg2
import pandas as pd
import configs.serverconfig as config


class DBConnection:
    """A class that knows how to connect to and manage connections to the DB
    """

    def __init__(
        self,
        host=config.DB_HOST,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        database=config.DB_DATABASE,
        port=config.DB_PORT,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port

    def open(self):
        """Opens a connection to our DB
        """
        self.conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
        )
        self.conn.set_session(autocommit=True)
        self.cur = self.conn.cursor()

    def close(self):
        """Closes the connection to our DB
        """
        self.cur.close()
        self.conn.close()

    def execute(self, command):
        """Runs a command on the DB
        
        Arguments:
            command {str} -- any SQL query to be run on the DB
        """
        if config.DEBUG and not config.DEBUG_DB_ACCESS:
            pass
        else:
            if config.VERBOSE:
                print("Running SQL: ", command)
            self.cur.execute(command)

    def read_field_from_field_value_table(self, r, f, v, tbl):
        """Read back a single column's list of rows from the DB based on query field+value
        
        Arguments:
            r {str} -- READ column name to return
            f {str} -- FIELD column name to search for VALUE
            v {str} -- VALUE any data that you are filtering for
            tbl {str} -- name of the table you want to filter
        
        Returns:
            list -- the READ field from a list of objects (one for each row that meets FIELD+VALUE filter)
        """
        res = None
        cmd = f"""
            SELECT {r} FROM {tbl}
            WHERE {f} = {v}
        """
        # print(cmd)
        self.open()
        try:
            res = pd.read_sql_query(cmd, self.conn)
        except (pd.io.sql.DatabaseError) as e:  # pylint: disable=no-member
            print("ERROR:", e)
        self.close()
        return res

    def insert_dict_as_row(self, dct, tbl):
        """Push a dictionary into a new row into any table on the DB
        
        Arguments:
            dct {dict} -- a dictionary with keys for any columns to be added
            tbl {str} -- name of the table to be added to
        """
        if config.VERBOSE:
            print("Inserting into: ", tbl)
        keys, values = zip(*dct.items())
        cmd = f"""
            INSERT INTO {tbl}({', '.join(keys)})
            VALUES({', '.join([str(v) for v in values])})
        """
        # print(cmd)
        self.open()
        try:
            self.execute(cmd)
            if config.VERBOSE:
                if config.DEBUG_DB_ACCESS:
                    print(f"writing to table {tbl}")
                else:
                    print(f"DB access turned off; would write to table {tbl}")
        except psycopg2.errors.UniqueViolation as e:  # pylint: disable=no-member
            print("ERROR:", e)
        self.close()
