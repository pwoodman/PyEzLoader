import unittest
from unittest.mock import patch, Mock
from utilities import trigger_dbt_run, log_info, log_warning, log_error

class TestTriggerDbtRun(unittest.TestCase):

    @patch('utilities.requests.post')
    def test_trigger_dbt_run_success(self, mock_post):
        # Set up the mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        # Call the function
        trigger_dbt_run('http://test-url.com', 'test_job_id', 'test_account_id')

        # Assert that requests.post was called with the correct arguments
        mock_post.assert_called_once_with(
            'http://test-url.com',
            json={'job_id': 'test_job_id', 'account_id': 'test_account_id'},
            headers={'Content-Type': 'application/json'}
        )

    @patch('utilities.requests.post')
    @patch('utilities.log_warning')
    def test_trigger_dbt_run_failure(self, mock_log_warning, mock_post):
        # Set up the mock response
        mock_response = Mock()
        mock_response.status_code = 400
        mock_post.return_value = mock_response

        # Call the function
        trigger_dbt_run('http://test-url.com', 'test_job_id', 'test_account_id')

        # Assert that log_warning was called with the correct message
        mock_log_warning.assert_called_once_with('Failed to trigger dbt run, status code: 400')

    @patch('utilities.requests.post')
    @patch('utilities.log_error')
    def test_trigger_dbt_run_exception(self, mock_log_error, mock_post):
        # Set up the mock to raise an exception
        mock_post.side_effect = Exception('Test exception')

        # Call the function
        trigger_dbt_run('http://test-url.com', 'test_job_id', 'test_account_id')

        # Assert that log_error was called with the correct message
        mock_log_error.assert_called_once_with('Error triggering dbt run: Test exception')

    def test_trigger_dbt_run_no_url(self):
        # Call the function with no URL
        trigger_dbt_run(None, 'test_job_id', 'test_account_id')
        # No assertions needed, function should simply not do anything

if __name__ == '__main__':
    unittest.main()