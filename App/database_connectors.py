# database_connectors.py

import pandas as pd
import psycopg2
from psycopg2 import sql
import pyodbc
from utilities import logger

class DatabaseError(Exception):
    pass

class PostgresSQLHandler:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def _connect(self):
        try:
            return psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL database: {e}")
            raise DatabaseError(f"Connection failed: {e}")

    def _schema_exists(self, conn, schema):
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.schemata
            WHERE schema_name = %s
        """, (schema,))
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        return exists

    def _table_exists(self, conn, schema, table):
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s
        """, (schema, table))
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        return exists

    def _create_table(self, conn, schema, table, columns):
        cursor = conn.cursor()
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                {})
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(', ').join(sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns)
        )
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()

    def read_data(self, schema, table):
        conn = self._connect()
        try:
            if not self._schema_exists(conn, schema):
                raise DatabaseError(f"Schema '{schema}' does not exist")
            if not self._table_exists(conn, schema, table):
                raise DatabaseError(f"Table '{schema}.{table}' does not exist")
            
            query = sql.SQL("SELECT * FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            df = pd.read_sql(query, conn)
            return df
        finally:
            conn.close()

    def write_data(self, data, schema, table, mode='append'):
        conn = self._connect()
        try:
            if not self._schema_exists(conn, schema):
                raise DatabaseError(f"Schema '{schema}' does not exist")
            
            if not self._table_exists(conn, schema, table):
                self._create_table(conn, schema, table, data.columns)
                logger.info(f"Table {schema}.{table} created.")

            if mode == 'append':
                data.to_sql(table, conn, schema=schema, if_exists='append', index=False)
            elif mode == 'replace':
                self.truncate_table(schema, table)
                data.to_sql(table, conn, schema=schema, if_exists='append', index=False)
            
            self._validate_write(conn, schema, table, len(data), mode)
        finally:
            conn.close()

    def _validate_write(self, conn, schema, table, expected_rows):
        cursor = conn.cursor()
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        ))
        actual_rows = cursor.fetchone()[0]
        cursor.close()
        if mode == 'append':
            logger.info(f"Data written successfully. Rows: {expected_rows}")
        else:
            if actual_rows != expected_rows:
                raise DatabaseError(f"Data write validation failed. Expected {expected_rows} rows, found {actual_rows}")

    def truncate_table(self, schema, table):
        conn = self._connect()
        try:
            if not self._schema_exists(conn, schema):
                raise DatabaseError(f"Schema '{schema}' does not exist")
            if not self._table_exists(conn, schema, table):
                raise DatabaseError(f"Table '{schema}.{table}' does not exist")
            
            cursor = conn.cursor()
            truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            cursor.execute(truncate_query)
            conn.commit()
            cursor.close()
            
            self._validate_truncate(conn, schema, table)
        finally:
            conn.close()

    def _validate_truncate(self, conn, schema, table):
        cursor = conn.cursor()
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        ))
        row_count = cursor.fetchone()[0]
        cursor.close()
        if row_count != 0:
            raise DatabaseError(f"Truncate operation failed. Table {schema}.{table} still contains {row_count} rows")

    def drop_table(self, schema, table):
        conn = self._connect()
        try:
            if not self._schema_exists(conn, schema):
                raise DatabaseError(f"Schema '{schema}' does not exist")
            
            cursor = conn.cursor()
            drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table)
            )
            cursor.execute(drop_query)
            conn.commit()
            cursor.close()
            
            self._validate_drop(conn, schema, table)
        finally:
            conn.close()

    def _validate_drop(self, conn, schema, table):
        if self._table_exists(conn, schema, table):
            raise DatabaseError(f"Drop operation failed. Table {schema}.{table} still exists")

    def append_data(self, data, schema, table):
        self.write_data(data, schema, table, mode='append')

    def truncate_and_load(self, data, schema, table):
        self.truncate_table(schema, table)
        self.write_data(data, schema, table, mode='append')

    def drop_and_load(self, data, schema, table):
        self.drop_table(schema, table)
        self.write_data(data, schema, table, mode='append')


class MSSQLHandler:
    def __init__(self, server, database, username, password, driver='{ODBC Driver 17 for SQL Server}'):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver

    def _connect(self):
        try:
            conn_str = f"""
            DRIVER={self.driver};
            SERVER={self.server};
            DATABASE={self.database};
            UID={self.username};
            PWD={self.password};
            """
            return pyodbc.connect(conn_str)
        except pyodbc.Error as e:
            logger.error(f"Failed to connect to MSSQL database: {e}")
            raise DatabaseError(f"Connection failed: {e}")

    def _schema_exists(self, conn, schema):
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM sys.schemas WHERE name = '{schema}'")
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        return exists

    def _table_exists(self, conn, schema, table):
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """)
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        return exists

    def _create_table(self, conn, schema, table, columns):
        cursor = conn.cursor()
        create_schema_query = f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}')
        BEGIN
            EXEC('CREATE SCHEMA {schema}')
        END
        """
        cursor.execute(create_schema_query)
        conn.commit()

        create_table_query = f"""
        CREATE TABLE {schema}.{table} (
            {', '.join([f'[{col}] NVARCHAR(MAX)' for col in columns])}
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        logger.info(f"Table {schema}.{table} created.")

    def read_data(self, schema, table):
        conn = self._connect()
        try:
            if not self._schema_exists(conn, schema):
                raise DatabaseError(f"Schema '{schema}' does not exist")
            if not self._table_exists(conn, schema, table):
                raise DatabaseError(f"Table '{schema}.{table}' does not exist")
            
            query = f"SELECT * FROM {schema}.{table}"
            df = pd.read_sql(query, conn)
            return df
        finally:
            conn.close()

    def write_data(self, data, schema, table, mode='append'):
        conn = self._connect()
        try:
            if not self._schema_exists(conn, schema):
                raise DatabaseError(f"Schema '{schema}' does not exist")
            
            if not self._table_exists(conn, schema, table):
                self._create_table(conn, schema, table, data.columns)

            data = self._clean_data(data)

            if mode == 'append':
                self._insert_data(conn, data, schema, table)
            elif mode == 'replace':
                self.truncate_table(schema, table)
                self._insert_data(conn, data, schema, table)
            
            self._validate_write(conn, schema, table, len(data),mode)
        finally:
            conn.close()

    def _clean_data(self, data):
        data.fillna(value={col: 0 if data[col].dtype.kind in 'biufc' else '' for col in data.columns}, inplace=True)
        float_columns = data.select_dtypes(include=['float64']).columns
        data[float_columns] = data[float_columns].round(2)
        return data

    def _insert_data(self, conn, data, schema, table):
        cursor = conn.cursor()
        cursor.fast_executemany = True
        columns = ', '.join([f'[{col}]' for col in data.columns])
        placeholders = ', '.join(['?' for _ in data.columns])
        insert_query = f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})"
        
        batch_size = 1000  # Adjust this based on your data size and available memory
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i:i+batch_size].values.tolist()
            try:
                cursor.executemany(insert_query, batch)
                conn.commit()
            except Exception as e:
                logger.error(f"Error inserting batch starting at row {i}: {str(e)}")
        
        cursor.close()

    def _validate_write(self, conn, schema, table, expected_rows, mode):
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        actual_rows = cursor.fetchone()[0]
        cursor.close()
        if mode == 'append':
            logger.info(f"Data written successfully. Rows: {actual_rows}")
        else:
            if actual_rows != expected_rows:
                raise DatabaseError(f"Data write validation failed. Expected {expected_rows} rows, found {actual_rows}")

    def truncate_table(self, schema, table):
        conn = self._connect()
        try:
            if not self._schema_exists(conn, schema):
                raise DatabaseError(f"Schema '{schema}' does not exist")
            if not self._table_exists(conn, schema, table):
                raise DatabaseError(f"Table '{schema}.{table}' does not exist")
            
            cursor = conn.cursor()
            truncate_query = f"TRUNCATE TABLE {schema}.{table}"
            cursor.execute(truncate_query)
            conn.commit()
            cursor.close()
            
            self._validate_truncate(conn, schema, table)
        finally:
            conn.close()

    def _validate_truncate(self, conn, schema, table):
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        row_count = cursor.fetchone()[0]
        cursor.close()
        if row_count != 0:
            raise DatabaseError(f"Truncate operation failed. Table {schema}.{table} still contains {row_count} rows")

    def drop_table(self, schema, table):
        conn = self._connect()
        try:
            if not self._schema_exists(conn, schema):
                raise DatabaseError(f"Schema '{schema}' does not exist")
            
            cursor = conn.cursor()
            drop_query = f"DROP TABLE IF EXISTS {schema}.{table}"
            cursor.execute(drop_query)
            conn.commit()
            cursor.close()
            
            self._validate_drop(conn, schema, table)
        finally:
            conn.close()

    def _validate_drop(self, conn, schema, table):
        if self._table_exists(conn, schema, table):
            raise DatabaseError(f"Drop operation failed. Table {schema}.{table} still exists")

    def append_data(self, data, schema, table):
        self.write_data(data, schema, table, mode='append')

    def truncate_and_load(self, data, schema, table):
        self.truncate_table(schema, table)
        self.write_data(data, schema, table, mode='append')

    def drop_and_load(self, data, schema, table):
        self.drop_table(schema, table)
        self.write_data(data, schema, table, mode='append')