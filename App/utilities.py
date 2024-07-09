import logging
import smtplib
import requests
import os
import yaml
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_start_step(step_name: str):
    logging.info(f"Starting step: {step_name}")

def log_end_step(step_name: str):
    logging.info(f"Completed step: {step_name}")

def log_info(message: str):
    logging.info(message)

def log_warning(message: str):
    logging.warning(message)

def log_error(message: str):
    logging.error(message)

def send_email(settings: Dict[str, Any], subject: str, body: str):
    try:
        msg = MIMEMultipart()
        msg['From'] = settings['sender_email']
        msg['To'] = settings['recipient_email']
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(settings['smtp_server'], settings['smtp_port']) as server:
            server.starttls()
            server.login(settings['sender_email'], settings['sender_password'])
            server.send_message(msg)
        log_info(f"Sent email to {settings['recipient_email']}")
    except Exception as e:
        log_error(f"Failed to send email: {e}")

def trigger_dbt_run(url: str, job_id: str, account_id: str):
    try:
        headers = {
            'Authorization': f'Token {os.getenv("DBT_CLOUD_API_TOKEN")}',
            'Content-Type': 'application/json'
        }
        payload = {'cause': 'Triggered by data pipeline'}
        response = requests.post(f'{url}/accounts/{account_id}/jobs/{job_id}/run/', headers=headers, json=payload)
        response.raise_for_status()
        log_info("Successfully triggered dbt run")
    except requests.RequestException as e:
        log_error(f"Error triggering dbt run: {e}")

class UtilityLoader:
    def __init__(self, utilities_folder: str):
        self.utilities = self.load_utilities(utilities_folder)

    def load_utilities(self, folder: str) -> Dict[str, Any]:
        utilities = {}
        for filename in os.listdir(folder):
            if filename.endswith('.yaml'):
                with open(os.path.join(folder, filename), 'r') as file:
                    utility_config = yaml.safe_load(file)
                    utilities[utility_config['name']] = utility_config
        return utilities

    def get_utility(self, utility_name: str) -> Dict[str, Any]:
        if utility_name not in self.utilities:
            raise ValueError(f"Utility {utility_name} not found")
        return self.utilities[utility_name]

def run_dbt_if_configured(config: Dict[str, Any], utility_loader: UtilityLoader):
    dbt_utility_name = config.get('utilities', {}).get('run_dbt', {}).get('job_name')
    if not dbt_utility_name:
        log_info("No dbt configuration provided, skipping dbt run")
        return

    try:
        dbt_utility = utility_loader.get_utility(dbt_utility_name)
        url = dbt_utility.get('url')
        job_id = dbt_utility.get('job_id')
        account_id = dbt_utility.get('account_id')

        if not all([url, job_id, account_id]):
            log_error("Incomplete dbt configuration, skipping dbt run")
            return

        trigger_dbt_run(url, job_id, account_id)
    except Exception as e:
        log_error(f"Failed to trigger dbt run: {e}")

def send_email_if_configured(config: Dict[str, Any], utility_loader: UtilityLoader, subject: str, body: str):
    email_utility_name = config.get('utilities', {}).get('email_notification', {}).get('config_name')
    if not email_utility_name:
        log_info("No email configuration provided, skipping email notification")
        return

    try:
        email_settings = utility_loader.get_utility(email_utility_name)
        send_email(email_settings, subject, body)
    except Exception as e:
        log_error(f"Failed to send email: {e}")

# Example usage:
if __name__ == "__main__":
    logging.info("Script started")
    
    # Load configuration from a file
    config_file_path = "path_to_config.yaml"
    with open(config_file_path, 'r') as file:
        config = yaml.safe_load(file)
    
    utility_loader = UtilityLoader(config['utilities_folder'])

    run_dbt_if_configured(config, utility_loader)
    send_email_if_configured(config, utility_loader, "Test Subject", "This is a test email body")

    logging.info("Script completed")
