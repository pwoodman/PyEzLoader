import logging
import smtplib
import requests
import os
import yaml
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def log_start_step(step_name):
    logging.info(f"Starting step: {step_name}")

def log_end_step(step_name):
    logging.info(f"Completed step: {step_name}")

def log_info(message):
    logging.info(message)

def log_warning(message):
    logging.warning(message)

def log_error(message):
    logging.error(message)

def load_email_settings(folder, email_name):
    if email_name:
        email_file = os.path.join(folder, f"{email_name}.yaml")
        if os.path.exists(email_file):
            with open(email_file, 'r') as file:
                return yaml.safe_load(file)
    return {}

def send_email(email_settings, subject, body):
    if not email_settings:
        logging.warning("No email settings provided.")
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = email_settings['from']
        msg['To'] = email_settings['to']
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(email_settings['smtp_server'], email_settings['smtp_port'])
        server.starttls()
        server.login(email_settings['from'], email_settings['password'])
        text = msg.as_string()
        server.sendmail(email_settings['from'], email_settings['to'], text)
        server.quit()
        logging.info(f"Sent email to {email_settings['to']}")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")

def trigger_dbt_run(dbt_run_url):
    if dbt_run_url:
        try:
            response = requests.get(dbt_run_url)
            if response.status_code == 200:
                log_info("Successfully triggered dbt run")
            else:
                log_warning(f"Failed to trigger dbt run, status code: {response.status_code}")
        except Exception as e:
            log_error(f"Error triggering dbt run: {e}")
