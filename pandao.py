# panda Dao(Data Access Object)

import sqlite3
import pandas as pd


class Dao:
    """
    Dao(Data Access Object): is a class for saving the records of the avro to the sqlite3
    """

    def __init__(self, db_name, table_name, buffer_max):
        self.table_name = table_name
        conn = sqlite3.connect(db_name, isolation_level='EXCLUSIVE')
        self.conn = conn
        cursor = conn.cursor()
        self._c = cursor
        self._buffer_max = buffer_max
        self._buffer = []

    def insert_all_records(self, records, lock):
        df = pd.DataFrame.from_records(records)
        lock.acquire()
        try:
            pd.DataFrame.to_sql(df, name=self.table_name, con=self.conn,
                                if_exists='append', index=False)
        finally:
            lock.release()

    @classmethod
    def load_table(cls, db_name, table_name, buffer_max=10000):
        if table_name == 'Alexa' or table_name == 'Umbrella':
            return cls(db_name, table_name, buffer_max)
        else:
            raise Exception("only support Alexa and Umbrella")

    def flush(self, lock):
        self.insert_all_records(self._buffer, lock)
        self._buffer = []

    def insert_data(self, data, lock):
        self._buffer.append(data)
        if len(self._buffer) >= self._buffer_max:
            self.flush(lock)

    def read_data(self, sql_command, chunksize=0):
        if chunksize != 0:
            return pd.read_sql(sql_command, self.conn, chunksize=chunksize)
        else:
            return pd.read_sql(sql_command, self.conn)

    def create_column(self, column_name, data_type):
        sql_command = "alter table %s add '%s' %s;" % (self.table_name, column_name, data_type)
        self._c.execute(sql_command)
        self.conn.commit()
