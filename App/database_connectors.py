#start of database_connectors.py
import pandas as pd
import psycopg2
from psycopg2 import sql

class PostgresSQLHandler:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def _connect(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def read_data(self, schema, table):
        query = sql.SQL("SELECT * FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        conn = self._connect()
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def write_data(self, data, schema, table, mode='append'):
        conn = self._connect()
        cursor = conn.cursor()
        # Create table if it does not exist
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                {} TEXT
            )
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier("dummy_column")
        )
        cursor.execute(create_table_query)
        conn.commit()

        # Remove the dummy_column after creation
        cursor.execute(sql.SQL("ALTER TABLE {}.{} DROP COLUMN IF EXISTS {}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier("dummy_column")
        ))
        conn.commit()

        if mode == 'append':
            data.to_sql(table, conn, schema=schema, if_exists='append', index=False)
        elif mode == 'replace':
            data.to_sql(table, conn, schema=schema, if_exists='replace', index=False)

        cursor.close()
        conn.close()

    def truncate_table(self, schema, table):
        conn = self._connect()
        cursor = conn.cursor()
        truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        cursor.execute(truncate_query)
        conn.commit()
        cursor.close()
        conn.close()

    def drop_table(self, schema, table):
        conn = self._connect()
        cursor = conn.cursor()
        drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        cursor.execute(drop_query)
        conn.commit()
        cursor.close()
        conn.close()

    def append_data(self, data, schema, table):
        self.write_data(data, schema, table, mode='append')

    def truncate_and_load(self, data, schema, table):
        self.truncate_table(schema, table)
        self.write_data(data, schema, table, mode='replace')

    def drop_and_load(self, data, schema, table):
        self.drop_table(schema, table)
        self.write_data(data, schema, table, mode='replace')

class MSSQLHandler:
    def __init__(self, server, database, username, password, driver='{ODBC Driver 17 for SQL Server}'):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver

    def _connect(self):
        conn_str = f"""
        DRIVER={self.driver};
        SERVER={self.server};
        DATABASE={self.database};
        UID={self.username};
        PWD={self.password};
        """
        return pyodbc.connect(conn_str)

    def read_data(self, schema, table):
        query = f"SELECT * FROM {schema}.{table}"
        conn = self._connect()
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def write_data(self, data, schema, table, mode='append'):
        conn = self._connect()
        cursor = conn.cursor()
        
        # Create table if it does not exist
        if not self._table_exists(conn, schema, table):
            create_table_query = f"""
            CREATE TABLE {schema}.{table} (
                {', '.join([f'[{col}] NVARCHAR(MAX)' for col in data.columns])}
            )
            """
            cursor.execute(create_table_query)
            conn.commit()

        if mode == 'append':
            self._insert_data(conn, data, schema, table)
        elif mode == 'replace':
            self._drop_table(conn, schema, table)
            create_table_query = f"""
            CREATE TABLE {schema}.{table} (
                {', '.join([f'[{col}] NVARCHAR(MAX)' for col in data.columns])}
            )
            """
            cursor.execute(create_table_query)
            conn.commit()
            self._insert_data(conn, data, schema, table)

        cursor.close()
        conn.close()

    def _table_exists(self, conn, schema, table):
        query = f"""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """
        cursor = conn.cursor()
        cursor.execute(query)
        exists = cursor.fetchone()[0] == 1
        cursor.close()
        return exists

    def _insert_data(self, conn, data, schema, table):
        cursor = conn.cursor()
        columns = ', '.join([f'[{col}]' for col in data.columns])
        placeholders = ', '.join(['?' for _ in data.columns])
        insert_query = f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})"
        
        for row in data.itertuples(index=False, name=None):
            cursor.execute(insert_query, row)
        
        conn.commit()
        cursor.close()

    def truncate_table(self, schema, table):
        conn = self._connect()
        cursor = conn.cursor()
        truncate_query = f"TRUNCATE TABLE {schema}.{table}"
        cursor.execute(truncate_query)
        conn.commit()
        cursor.close()
        conn.close()

    def drop_table(self, schema, table):
        conn = self._connect()
        cursor = conn.cursor()
        drop_query = f"DROP TABLE IF EXISTS {schema}.{table}"
        cursor.execute(drop_query)
        conn.commit()
        cursor.close()
        conn.close()

    def append_data(self, data, schema, table):
        self.write_data(data, schema, table, mode='append')

    def truncate_and_load(self, data, schema, table):
        self.truncate_table(schema, table)
        self.write_data(data, schema, table, mode='replace')

    def drop_and_load(self, data, schema, table):
        self.drop_table(schema, table)
        self.write_data(data, schema, table, mode='replace')

#end of database_connectors.py