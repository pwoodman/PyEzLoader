import io
import logging
from typing import Dict, Any, Optional, Union
from contextlib import contextmanager

import pandas as pd
import yaml
import chardet
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine.base import Engine

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseConnector:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @contextmanager
    def _connect(self):
        raise NotImplementedError("_connect method not implemented")

    def _execute_transaction(self, conn, operation, transaction_mode='read_write'):
        if transaction_mode in ('write', 'read_write'):
            try:
                conn.execute(text("START TRANSACTION"))
                operation(conn)
                conn.execute(text("COMMIT"))
                logger.info("Transaction committed successfully.")
            except Exception as e:
                conn.execute(text("ROLLBACK"))
                logger.error(f"Transaction rolled back. Error: {e}")
                raise

    def read(self, query: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError("read method not implemented")

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append') -> None:
        raise NotImplementedError("write method not implemented")

    def table_exists(self, table_name: str) -> bool:
        raise NotImplementedError("table_exists method not implemented")

    def execute_query(self, query: str) -> None:
        with self._connect() as conn:
            self._execute_transaction(conn, lambda c: c.execute(text(query)))

class FileConnector(BaseConnector):
    def read(self, query: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError("read method not implemented")

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append') -> None:
        raise NotImplementedError("write method not implemented")

class ExcelConnector(FileConnector):
    def read(self, query: Optional[str] = None) -> pd.DataFrame:
        try:
            df = pd.read_excel(
                self.config['file_path'],
                sheet_name=self.config.get('sheet_name'),
                header=self.config.get('header_start_row', 0)
            )
            logger.info(f"Successfully read data from Excel file: {self.config['file_path']}.")
            return df
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append') -> None:
        try:
            df.to_excel(self.config['file_path'], index=False)
            logger.info(f"Successfully wrote data to Excel file: {self.config['file_path']}.")
        except Exception as e:
            logger.error(f"Error writing to Excel file: {e}")
            raise

class CSVConnector(FileConnector):
    def read(self, query: Optional[str] = None) -> pd.DataFrame:
        try:
            file_path = self.config['file_path']
            encoding = self.config.get('encoding', 'utf-8')
            delimiter = self.config.get('delimiter', ',')
            if isinstance(delimiter, str) and delimiter.startswith('"') and delimiter.endswith('"'):
                delimiter = delimiter[1:-1]
            df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, engine='python', on_bad_lines='warn')
            logger.info(f"Successfully read data from file: {file_path} with encoding {encoding} and delimiter '{delimiter}'.")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append') -> None:
        try:
            file_path = self.config['file_path']
            encoding = self.config.get('encoding', 'utf-8')
            delimiter = self.config.get('delimiter', ',')
            if isinstance(delimiter, str) and delimiter.startswith('"') and delimiter.endswith('"'):
                delimiter = delimiter[1:-1]
            df.to_csv(file_path, index=False, sep=delimiter, encoding=encoding)
            logger.info(f"Successfully wrote data to file: {file_path} with encoding {encoding} and delimiter '{delimiter}'.")
        except Exception as e:
            logger.error(f"Error writing to CSV file: {e}")
            raise

    def _detect_encoding(self, file_path: str) -> str:
        with open(file_path, 'rb') as f:
            rawdata = f.read(10000)
        return chardet.detect(rawdata)['encoding']


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
            connect_args={'sslmode': 'require'}
        )

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        schema_prefix = f"{schema}." if schema else ""
        with self._connect() as connection:
            result = connection.execute(text(f"SELECT to_regclass('{schema_prefix}{table_name}') IS NOT NULL")).scalar()
            return bool(result)

    def truncate_table(self, table_name: str, schema: Optional[str] = None) -> None:
        if self.table_exists(table_name, schema):
            schema_prefix = f"{schema}." if schema else ""
            self.execute_query(f"TRUNCATE TABLE {schema_prefix}{table_name}")
            logger.info(f"Successfully truncated {schema_prefix}{table_name} table.")
        else:
            logger.warning(f"Table {table_name} does not exist. Skipping truncate operation.")

    def drop_table(self, table_name: str, schema: Optional[str] = None) -> None:
        schema_prefix = f"{schema}." if schema else ""
        self.execute_query(f"DROP TABLE IF EXISTS {schema_prefix}{table_name}")
        logger.info(f"Successfully dropped {schema_prefix}{table_name} table.")

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append', schema: Optional[str] = None) -> None:
        schema_prefix = f"{schema}." if schema else ""
        if_exists = 'replace' if mode in ('truncate', 'replace') else 'append'
        with self._connect() as connection:
            if mode == 'truncate':
                self.truncate_table(table_name, schema)
            elif mode == 'replace':
                self.drop_table(table_name, schema)
            df.to_sql(table_name, connection, schema=schema, if_exists=if_exists, index=False)
        logger.info(f"Successfully wrote data to {schema_prefix}{table_name} table.")




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

class DatabaseConnector:
    CONNECTOR_MAP = {
        'Excel': ExcelConnector,
        'CSV': CSVConnector,
        'PostgreSQL': PostgreSQLConnector,
        'MySQL': MySQLConnector,
        'Redshift': RedshiftConnector,
        'MSSQL': MSSQLConnector,
        'Oracle': OracleConnector,
        'SQLite': SQLiteConnector,
        'ODBC': ODBCConnector
    }

    @staticmethod
    def get_connector(connector_type: str, config: Dict[str, Any]) -> BaseConnector:
        connector_class = DatabaseConnector.CONNECTOR_MAP.get(connector_type)
        if not connector_class:
            raise ValueError(f"Unsupported connector type: {connector_type}")
        return connector_class(config)

    @staticmethod
    def read(connector_type: str, config: Dict[str, Any], query: Optional[str] = None) -> pd.DataFrame:
        """
        Read data from the specified connector.

        :param connector_type: Type of the connector (e.g., 'PostgreSQL', 'CSV')
        :param config: Configuration dictionary for the connector
        :param query: SQL query to execute (for SQL-based connectors)
        :return: pandas DataFrame with the query results
        """
        connector = DatabaseConnector.get_connector(connector_type, config)
        return connector.read(query)

    @staticmethod
    def append(connector_type: str, config: Dict[str, Any], df: pd.DataFrame, table_name: str) -> None:
        """
        Append data to an existing table.

        :param connector_type: Type of the connector
        :param config: Configuration dictionary for the connector
        :param df: pandas DataFrame to append
        :param table_name: Name of the target table
        """
        connector = DatabaseConnector.get_connector(connector_type, config)
        connector.write(df, table_name, mode='append')
        logger.info(f"Successfully appended data to {table_name} table.")

    @staticmethod
    def truncate_and_load(connector_type: str, config: Dict[str, Any], df: pd.DataFrame, table_name: str) -> None:
        """
        Truncate an existing table and load new data.

        :param connector_type: Type of the connector
        :param config: Configuration dictionary for the connector
        :param df: pandas DataFrame to load
        :param table_name: Name of the target table
        """
        connector = DatabaseConnector.get_connector(connector_type, config)
        connector.truncate_table(table_name)
        connector.write(df, table_name, mode='append')
        logger.info(f"Successfully truncated and loaded data to {table_name} table.")

    @staticmethod
    def drop_and_load(connector_type: str, config: Dict[str, Any], df: pd.DataFrame, table_name: str) -> None:
        """
        Drop an existing table and create a new one with the provided data.

        :param connector_type: Type of the connector
        :param config: Configuration dictionary for the connector
        :param df: pandas DataFrame to load
        :param table_name: Name of the target table
        """
        connector = DatabaseConnector.get_connector(connector_type, config)
        connector.drop_table(table_name)
        connector.write(df, table_name, mode='replace')
        logger.info(f"Successfully dropped and recreated {table_name} table with new data.")
