import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from connectors import DatabaseConnector, ExcelConnector, CSVConnector, PostgreSQLConnector, MySQLConnector, RedshiftConnector, MSSQLConnector, OracleConnector, SQLiteConnector, ODBCConnector
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TestConnectors(unittest.TestCase):
    
    configs = {
        'Excel': {'type': 'Excel', 'file_path': 'dummy.xlsx'},
        'CSV': {'type': 'CSV', 'file_path': 'dummy.csv'},
        'PostgreSQL': {'type': 'PostgreSQL', 'user': 'user', 'password': 'pass', 'host': 'localhost', 'dbname': 'db'},
        'MySQL': {'type': 'MySQL', 'user': 'user', 'password': 'pass', 'host': 'localhost', 'dbname': 'db'},
        'Redshift': {'type': 'Redshift', 'user': 'user', 'password': 'pass', 'host': 'localhost', 'dbname': 'db'},
        'MSSQL': {'type': 'MSSQL', 'user': 'user', 'password': 'pass', 'host': 'localhost', 'dbname': 'db'},
        'Oracle': {'type': 'Oracle', 'user': 'user', 'password': 'pass', 'host': 'localhost', 'service_name': 'orcl'},
        'SQLite': {'type': 'SQLite', 'file_path': 'dummy.db'},
        'ODBC': {'type': 'ODBC', 'user': 'user', 'password': 'pass', 'dsn': 'dummy_dsn'}
    }

    @staticmethod
    def is_sql_based(connector_type):
        return connector_type not in ['Excel', 'CSV']

    def dynamic_test_generator(self, connector_type, method_name):
        def test(self):
            config = self.configs[connector_type]

            if method_name == 'read':
                with patch(f'connectors.{connector_type}Connector.read', return_value=pd.DataFrame()) as mock_read:
                    result = DatabaseConnector.read(connector_type, config, "SELECT * FROM dummy_table")
                    self.assertIsInstance(result, pd.DataFrame)
                    mock_read.assert_called_once()

            elif method_name == 'append':
                df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
                with patch(f'connectors.{connector_type}Connector.write') as mock_write:
                    DatabaseConnector.append(connector_type, config, df, 'dummy_table')
                    mock_write.assert_called_once_with(df, 'dummy_table', mode='append')

            elif method_name == 'truncate_and_load' and self.is_sql_based(connector_type):
                df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
                with patch(f'connectors.{connector_type}Connector.truncate_table') as mock_truncate, \
                     patch(f'connectors.{connector_type}Connector.write') as mock_write:
                    DatabaseConnector.truncate_and_load(connector_type, config, df, 'dummy_table')
                    mock_truncate.assert_called_once_with('dummy_table')
                    mock_write.assert_called_once_with(df, 'dummy_table', mode='append')

            elif method_name == 'drop_and_load' and self.is_sql_based(connector_type):
                df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
                with patch(f'connectors.{connector_type}Connector.drop_table') as mock_drop, \
                     patch(f'connectors.{connector_type}Connector.write') as mock_write:
                    DatabaseConnector.drop_and_load(connector_type, config, df, 'dummy_table')
                    mock_drop.assert_called_once_with('dummy_table')
                    mock_write.assert_called_once_with(df, 'dummy_table', mode='replace')

        return test

    @classmethod
    def add_dynamic_tests(cls):
        for connector_type in cls.configs.keys():
            test_methods = ['read', 'append']
            if cls.is_sql_based(connector_type):
                test_methods.extend(['truncate_and_load', 'drop_and_load'])

            for method in test_methods:
                test_name = f'test_{connector_type}_{method}'
                test_func = cls().dynamic_test_generator(connector_type, method)
                setattr(cls, test_name, test_func)

TestConnectors.add_dynamic_tests()

if __name__ == '__main__':
    unittest.main()