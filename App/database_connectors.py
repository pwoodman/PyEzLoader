from base_connectors import BaseConnector
from file_connectors import ExcelConnector, CSVConnector
from sql_connectors import (PostgreSQLConnector, MySQLConnector, RedshiftConnector, 
                            MSSQLConnector, OracleConnector, SQLiteConnector, ODBCConnector)
import pandas as pd
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

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
