from base_connectors import BaseConnector
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import pandas as pd
import logging
from typing import Dict, Any , Optional

logger = logging.getLogger(__name__)

class SQLConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.engine: Engine = self._create_engine()

    def _create_engine(self) -> Engine:
        raise NotImplementedError("_create_engine method not implemented")

    @contextmanager
    def _connect(self):
        connection = self.engine.connect()
        try:
            yield connection
        finally:
            connection.close()

    def read(self, query: str) -> pd.DataFrame:
        try:
            with self._connect() as connection:
                df = pd.read_sql(text(query), connection)
            logger.info("Successfully executed query: %s", query)
            return df
        except SQLAlchemyError as e:
            logger.error("Error executing query: %s", str(e))
            raise

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append') -> None:
        try:
            if_exists = 'replace' if mode in ('truncate', 'replace') else 'append'
            with self._connect() as connection:
                if mode == 'truncate':
                    self.truncate_table(table_name)
                elif mode == 'replace':
                    self.drop_table(table_name)
                df.to_sql(table_name, connection, if_exists=if_exists, index=False)
            logger.info(f"Successfully wrote data to {table_name} table.")
        except Exception as e:
            logger.error(f"Error writing to table: {e}")
            raise

    def truncate_table(self, table_name: str) -> None:
        if self.table_exists(table_name):
            self.execute_query(f"TRUNCATE TABLE {table_name}")
            logger.info(f"Successfully truncated {table_name} table.")
        else:
            logger.warning(f"Table {table_name} does not exist. Skipping truncate operation.")

    def drop_table(self, table_name: str) -> None:
        self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
        logger.info(f"Successfully dropped {table_name} table.")

    def __del__(self):
        if hasattr(self, 'engine'):
            self.engine.dispose()

class PostgreSQLConnector(SQLConnector):
    def _create_engine(self) -> Engine:
        connection_string = (
            f"postgresql+psycopg2://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config.get('port', 5432)}/"
            f"{self.config.get('dbname', 'postgres')}"
        )
        return create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800,
            connect_args={'sslmode': self.config.get('sslmode', 'prefer')}
        )

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        schema = schema or self.config.get('schema', 'public')
        query = text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = :schema AND table_name = :table_name
            )
        """)
        with self._connect() as connection:
            result = connection.execute(query, {'schema': schema, 'table_name': table_name}).scalar()
            return bool(result)

    def truncate_table(self, table_name: str, schema: Optional[str] = None) -> None:
        schema = schema or self.config.get('schema', 'public')
        if self.table_exists(table_name, schema):
            self.execute_query(f'TRUNCATE TABLE {schema}."{table_name}"')
            logger.info(f"Successfully truncated {schema}.{table_name} table.")
        else:
            logger.warning(f"Table {schema}.{table_name} does not exist. Skipping truncate operation.")

    def drop_table(self, table_name: str, schema: Optional[str] = None) -> None:
        schema = schema or self.config.get('schema', 'public')
        self.execute_query(f'DROP TABLE IF EXISTS {schema}."{table_name}"')
        logger.info(f"Successfully dropped {schema}.{table_name} table.")

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append', schema: Optional[str] = None) -> None:
        schema = schema or self.config.get('schema', 'public')
        if_exists = 'replace' if mode == 'replace' else 'append'
        
        with self._connect() as connection:
            if mode == 'truncate':
                self.truncate_table(table_name, schema)
                df.to_sql(table_name, connection, schema=schema, if_exists='append', index=False)
            elif mode == 'replace':
                self.drop_table(table_name, schema)
                df.to_sql(table_name, connection, schema=schema, if_exists='fail', index=False)
            else:
                df.to_sql(table_name, connection, schema=schema, if_exists=if_exists, index=False)
        
        logger.info(f"Successfully wrote data to {schema}.{table_name} table.")

class MySQLConnector(SQLConnector):
    def _create_engine(self) -> Engine:
        connection_string = (
            f"mysql+mysqlconnector://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config.get('port', 3306)}/"
            f"{self.config['dbname']}"
        )
        return create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800
        )

    def table_exists(self, table_name: str) -> bool:
        with self._connect() as connection:
            result = connection.execute(text(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")).scalar()
            return bool(result)

class RedshiftConnector(SQLConnector):
    def _create_engine(self) -> Engine:
        connection_string = (
            f"redshift+psycopg2://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config.get('port', 5439)}/"
            f"{self.config['dbname']}"
        )
        return create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800
        )

    def table_exists(self, table_name: str) -> bool:
        with self._connect() as connection:
            result = connection.execute(text(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'")).scalar()
            return bool(result)

class MSSQLConnector(SQLConnector):
    def _create_engine(self) -> Engine:
        connection_string = (
            f"mssql+pyodbc://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config.get('port', 1433)}/"
            f"{self.config['dbname']}?driver=ODBC+Driver+17+for+SQL+Server"
        )
        return create_engine(connection_string)

    def table_exists(self, table_name: str) -> bool:
        with self._connect() as connection:
            result = connection.execute(text(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")).scalar()
            return bool(result)

class OracleConnector(SQLConnector):
    def _create_engine(self) -> Engine:
        connection_string = (
            f"oracle+cx_oracle://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config.get('port', 1521)}/"
            f"{self.config['service_name']}"
        )
        return create_engine(connection_string)

    def table_exists(self, table_name: str) -> bool:
        with self._connect() as connection:
            result = connection.execute(text(f"SELECT 1 FROM all_tables WHERE table_name = '{table_name.upper()}'")).scalar()
            return bool(result)

class SQLiteConnector(SQLConnector):
    def _create_engine(self) -> Engine:
        return create_engine(f"sqlite:///{self.config['file_path']}")

    def table_exists(self, table_name: str) -> bool:
        with self._connect() as connection:
            result = connection.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")).scalar()
            return bool(result)

    def truncate_table(self, table_name: str) -> None:
        if self.table_exists(table_name):
            self.execute_query(f"DELETE FROM {table_name}")
            logger.info(f"Successfully truncated {table_name} table.")
        else:
            logger.warning(f"Table {table_name} does not exist. Skipping truncate operation.")

class ODBCConnector(SQLConnector):
    def _create_engine(self) -> Engine:
        connection_string = (
            f"mssql+pyodbc://{self.config['user']}:{self.config['password']}@"
            f"{self.config['dsn']}"
        )
        return create_engine(connection_string)

    def table_exists(self, table_name: str) -> bool:
        with self._connect() as connection:
            result = connection.execute(text(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")).scalar()
            return bool(result)
