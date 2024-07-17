import pandas as pd
import pyodbc
import psycopg2
from utilities import logger

class DatabaseError(Exception):
    """Custom exception for database errors."""
    pass

class MultiDB:
    def __init__(self, db_type, connection_string):
        self.db_type = db_type
        self.connection_string = connection_string
        self.conn = self.connect_to_db()
        self.cursor = self.conn.cursor()

    def connect_to_db(self):
        try:
            if self.db_type == 'mssql':
                return pyodbc.connect(self.connection_string)
            elif self.db_type == 'postgresql':
                return psycopg2.connect(self.connection_string)
            else:
                raise ValueError("Unsupported database type")
        except (pyodbc.OperationalError, psycopg2.OperationalError) as e:
            logger.error(f"Operational error while connecting to {self.db_type} database: {str(e)}")
            raise DatabaseError(f"Operational error: {str(e)}")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError) as e:
            logger.error(f"Programming error while connecting to {self.db_type} database: {str(e)}")
            raise DatabaseError(f"Programming error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while connecting to {self.db_type} database: {str(e)}")
            raise DatabaseError(f"Unexpected error: {str(e)}")

    def create_table_if_not_exists(self, table_name, columns):
        try:
            columns_with_types = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
            if self.db_type == 'mssql':
                create_table_query = f"""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
                CREATE TABLE {table_name} ({columns_with_types});
                """
            elif self.db_type == 'postgresql':
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types});
                """
            self.cursor.execute(create_table_query)
            self.conn.commit()
            logger.info(f"Table '{table_name}' created if not exists.")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError) as e:
            logger.error(f"Programming error while creating table '{table_name}': {str(e)}")
            raise DatabaseError(f"Programming error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while creating table '{table_name}': {str(e)}")
            raise DatabaseError(f"Unexpected error: {str(e)}")

    def read_data(self, table_name, conditions=""):
        try:
            query = f"SELECT * FROM {table_name} {conditions};"
            self.cursor.execute(query)
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            logger.info(f"Data read from table '{table_name}'.")
            return pd.DataFrame(rows, columns=columns)
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError) as e:
            logger.error(f"Programming error while reading data from table '{table_name}': {str(e)}")
            raise DatabaseError(f"Programming error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while reading data from table '{table_name}': {str(e)}")
            raise DatabaseError(f"Unexpected error: {str(e)}")

    def insert_data(self, table_name, df):
        try:
            data = [tuple(row) for row in df.itertuples(index=False, name=None)]
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns)) if self.db_type == 'postgresql' else ', '.join(['?'] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            self.cursor.executemany(insert_query, data)
            self.conn.commit()
            logger.info(f"Data inserted into table '{table_name}'.")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError) as e:
            logger.error(f"Programming error while inserting data into table '{table_name}': {str(e)}")
            raise DatabaseError(f"Programming error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while inserting data into table '{table_name}': {str(e)}")
            raise DatabaseError(f"Unexpected error: {str(e)}")

    def delete_data(self, table_name, conditions):
        try:
            delete_query = f"DELETE FROM {table_name} WHERE {conditions};"
            self.cursor.execute(delete_query)
            self.conn.commit()
            logger.info(f"Data deleted from table '{table_name}' where {conditions}.")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError) as e:
            logger.error(f"Programming error while deleting data from table '{table_name}': {str(e)}")
            raise DatabaseError(f"Programming error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while deleting data from table '{table_name}': {str(e)}")
            raise DatabaseError(f"Unexpected error: {str(e)}")

    def truncate_table(self, table_name):
        try:
            truncate_query = f"TRUNCATE TABLE {table_name};" if self.db_type == 'mssql' else f"TRUNCATE TABLE {table_name} RESTART IDENTITY;"
            self.cursor.execute(truncate_query)
            self.conn.commit()
            logger.info(f"Table '{table_name}' truncated.")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError) as e:
            logger.error(f"Programming error while truncating table '{table_name}': {str(e)}")
            raise DatabaseError(f"Programming error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while truncating table '{table_name}': {str(e)}")
            raise DatabaseError(f"Unexpected error: {str(e)}")

    def handle_dataframe(self, table_name, df, if_exists='append'):
        try:
            if if_exists == 'replace':
                self.truncate_table(table_name)
            self.insert_data(table_name, df)
            logger.info(f"DataFrame handled for table '{table_name}' with if_exists='{if_exists}'.")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError) as e:
            logger.error(f"Programming error while handling DataFrame for table '{table_name}': {str(e)}")
            raise DatabaseError(f"Programming error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while handling DataFrame for table '{table_name}': {str(e)}")
            raise DatabaseError(f"Unexpected error: {str(e)}")

    def close_connection(self):
        try:
            self.cursor.close()
            self.conn.close()
            logger.info("Database connection closed.")
        except (pyodbc.ProgrammingError, psycopg2.ProgrammingError) as e:
            logger.error(f"Programming error while closing database connection: {str(e)}")
            raise DatabaseError(f"Programming error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error while closing database connection: {str(e)}")
            raise DatabaseError(f"Unexpected error: {str(e)}")
