import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime
from pipelines import Pipeline, DatabaseConnector, UtilityLoader

class TestPipeline(unittest.TestCase):
    def setUp(self):
        self.config = {
            'name': 'test_pipeline',
            'source': {'connection': 'source_db', 'type': 'sql', 'query': 'SELECT * FROM test_table'},
            'target': {'connection': 'target_db', 'type': 'sql', 'database': 'test_db', 'schema': 'dbo', 'table_name': 'target_table', 'mode': 'replace'},
            'transformations': [
                {'type': 'add_timestamp', 'column_name': 'created_at'},
                {'type': 'rename_column', 'old_name': 'old_col', 'new_name': 'new_col'},
                {'type': 'standardize_phone', 'column_name': 'phone'},
                {'type': 'calculate_value', 'new_column': 'total', 'formula': 'col_a + col_b'}
            ]
        }
        
        # Mock the DatabaseConnector and UtilityLoader
        self.mock_connector = MagicMock(spec=DatabaseConnector)
        self.mock_utility_loader = MagicMock(spec=UtilityLoader)
        
        # Patch the DatabaseConnector and UtilityLoader in the Pipeline class

        with patch('pipelines.DatabaseConnector', return_value=self.mock_connector), \
            patch('pipelines.UtilityLoader', return_value=self.mock_utility_loader):
           self.pipeline = Pipeline(self.config)


    def test_validate_config(self):
        self.pipeline.validate_config()  # Should not raise an exception

        invalid_config = self.config.copy()
        del invalid_config['source']
        with self.assertRaises(ValueError):
            Pipeline(invalid_config)

    @patch('pandas.read_sql')
    def test_read_source(self, mock_read_sql):
        mock_read_sql.return_value = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        result = self.pipeline.read_source()
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)

    def test_transform(self):
        input_df = pd.DataFrame({
            'old_col': ['value1', 'value2'],
            'phone': ['123-456-7890', '987-654-3210'],
            'col_a': [1, 2],
            'col_b': [3, 4]
        })
        
        # Mock the connections dictionary
        self.pipeline.connector.connections = {
            'source_db': {'type': 'MS SQL Server', 'database': 'test_db', 'schema': 'dbo'}
        }
        
        with patch('pipelines.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
            result = self.pipeline.transform(input_df)
        
        self.assertIn('created_at', result.columns)
        self.assertIn('new_col', result.columns)
        self.assertIn('total', result.columns)
        self.assertEqual(result['total'].tolist(), [4, 6])
        self.assertEqual(result['phone'].tolist(), ['1234567890', '9876543210'])
        self.assertIn('source_connector_name', result.columns)
        self.assertIn('source_connector_type', result.columns)
        self.assertIn('database', result.columns)
        self.assertIn('schema', result.columns)
        self.assertIn('pipeline_run_timestamp', result.columns)

    def test_standardize_phone(self):
        self.assertEqual(self.pipeline.standardize_phone('123-456-7890'), '1234567890')
        self.assertEqual(self.pipeline.standardize_phone('(123) 456-7890'), '1234567890')
        self.assertEqual(self.pipeline.standardize_phone('123.456.7890'), '1234567890')

if __name__ == '__main__':
    unittest.main()