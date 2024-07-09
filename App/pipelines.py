import os
import pandas as pd
import yaml
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
import dotenv
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Import the utility functions
from utilities import (
    log_start_step, log_end_step, log_info, log_warning, log_error,
    load_email_settings, send_email, trigger_dbt_run
)

# Load environment variables
dotenv.load_dotenv()

class DatabaseConnector:
    def __init__(self, connections_folder: str):
        self.connections = self.load_connections(connections_folder)

    def load_connections(self, folder: str) -> Dict:
        connections = {}
        for filename in os.listdir(folder):
            if filename.endswith('.yaml'):
                with open(os.path.join(folder, filename), 'r') as file:
                    connection_config = yaml.safe_load(file)
                    connections[connection_config['name']] = connection_config
        return connections

    def get_connector(self, connection_name: str):
        if connection_name not in self.connections:
            raise ValueError(f"Connection {connection_name} not found")
        config = self.connections[connection_name]
        engine = create_engine(config['connection_string'])
        return engine.connect()

class UtilityLoader:
    def __init__(self, utilities_folder: str):
        self.utilities = self.load_utilities(utilities_folder)

    def load_utilities(self, folder: str) -> Dict:
        utilities = {}
        for filename in os.listdir(folder):
            if filename.endswith('.yaml'):
                with open(os.path.join(folder, filename), 'r') as file:
                    utility_config = yaml.safe_load(file)
                    utilities[utility_config['name']] = utility_config
        return utilities

    def get_utility(self, utility_name: str):
        if utility_name not in self.utilities:
            raise ValueError(f"Utility {utility_name} not found")
        return self.utilities[utility_name]

import os
import pandas as pd
import yaml
from datetime import datetime
from typing import Dict
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

from utilities import (
    log_start_step, log_end_step, log_info, log_warning, log_error,
    load_email_settings, send_email, trigger_dbt_run
)

class Pipeline:
    def __init__(self, config: Dict, connections_folder: str = 'Connections', utilities_folder: str = 'Utilities'):
        self.config = config
        self.validate_config()
        self.connector = DatabaseConnector(connections_folder)
        self.utility_loader = UtilityLoader(utilities_folder)
        
        # Set up source and target
        self.setup_source()
        self.setup_target()

    def validate_config(self):
        required_keys = ['name', 'source', 'target', 'transformations']
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration key: {key}")

    def setup_source(self):
        source_config = self.config['source']
        self.source_type = source_config['type']
        self.source_connection = source_config['connection']
        if self.source_type == 'sql':
            self.source_query = source_config['query']
        elif self.source_type in ['csv', 'excel']:
            self.source_file = source_config['file_path']
            self.source_sheet = source_config.get('sheet_name')

    def setup_target(self):
        target_config = self.config['target']
        self.target_type = target_config['type']
        self.target_connection = target_config['connection']
        self.target_mode = target_config['mode']
        if self.target_type == 'sql':
            self.target_database = target_config['database']
            self.target_schema = target_config['schema']
            self.target_table = target_config['table_name']
        elif self.target_type in ['csv', 'excel']:
            self.target_file = target_config['file_path']
            self.target_sheet = target_config.get('sheet_name')

    def run(self):
        try:
            log_start_step(f"Pipeline run: {self.config['name']}")
            source_data = self.read_source()
            log_info("Data read from source")

            transformed_data = self.transform(source_data)
            log_info("Data transformed")

            self.write_target(transformed_data)
            log_info("Data written to target")

            log_end_step(f"Pipeline run: {self.config['name']}")
            self.run_dbt_if_configured()
        except Exception as e:
            self.handle_error("Pipeline run failed", e)

    def read_source(self) -> pd.DataFrame:
        log_start_step("Reading source data")
        if self.source_type == 'sql':
            source_connector = self.connector.get_connector(self.source_connection)
            df = self.read_in_chunks(source_connector, self.source_query)
        elif self.source_type == 'csv':
            df = pd.read_csv(self.source_file)
        elif self.source_type == 'excel':
            df = pd.read_excel(self.source_file, sheet_name=self.source_sheet)
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")
        log_end_step("Reading source data")
        return df

    def write_target(self, df: pd.DataFrame):
        log_start_step("Writing target data")
        if self.target_type == 'sql':
            target_connector = self.connector.get_connector(self.target_connection)
            self.write_to_sql(target_connector, df, self.target_table, self.target_mode)
        elif self.target_type == 'csv':
            df.to_csv(self.target_file, index=False)
        elif self.target_type == 'excel':
            with pd.ExcelWriter(self.target_file, mode='a' if self.target_mode == 'append' else 'w') as writer:
                df.to_excel(writer, sheet_name=self.target_sheet, index=False)
        else:
            raise ValueError(f"Unsupported target type: {self.target_type}")
        log_end_step("Writing target data")

    def handle_error(self, error_type: str, error: Exception):
        log_error(f"{error_type}: {error}")
        self.send_email_if_configured(f"Pipeline Failure Alert - {error_type}", f"{error_type} in pipeline {self.config['name']}: {error}")

    def read_in_chunks(self, connector, query: str, chunksize: int = 10000) -> pd.DataFrame:
        dfs = []
        for chunk in pd.read_sql(query, con=connector, chunksize=chunksize):
            dfs.append(chunk)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def write_to_sql(self, connector, df: pd.DataFrame, table_name: str, mode: str):
        if mode not in ['append', 'replace']:
            raise ValueError("Invalid mode specified. Use 'append' or 'replace'.")
        df.to_sql(table_name, con=connector, schema=self.target_schema, if_exists=mode, index=False)

    def run_dbt_if_configured(self):
        dbt_utility_name = self.config.get('utilities', {}).get('run_dbt', {}).get('job_name')
        if not dbt_utility_name:
            log_info("No dbt configuration provided, skipping dbt run")
            return

        try:
            dbt_utility = self.utility_loader.get_utility(dbt_utility_name)
            
            url = dbt_utility.get('url')
            job_id = dbt_utility.get('job_id')
            account_id = dbt_utility.get('account_id')

            if not all([url, job_id, account_id]):
                log_error("Incomplete dbt configuration, skipping dbt run")
                return

            trigger_dbt_run(url, job_id, account_id)
        except Exception as e:
            self.handle_error("Failed to trigger dbt run", e)

    def send_email_if_configured(self, subject: str, body: str):
        email_utility_name = self.config.get('utilities', {}).get('email_notification', {}).get('config_name')
        if not email_utility_name:
            log_info("No email configuration provided, skipping email notification")
            return

        try:
            email_settings = self.utility_loader.get_utility(email_utility_name)
            send_email(email_settings, subject, body)
        except Exception as e:
            log_error(f"Failed to send email: {e}")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        log_start_step("Data transformation")
        for transformation in self.config.get('transformations', []):
            df = self.apply_transformation(df, transformation)

        source_conn = self.connector.connections[self.source_connection]
        df['source_connector_name'] = self.source_connection
        df['source_connector_type'] = source_conn['type']
        log_info(f"Added metadata columns: source_connector_name, source_connector_type")

        if source_conn['type'] == 'MS SQL Server':
            df['database'] = source_conn.get('database')
            df['schema'] = source_conn.get('schema')
            log_info(f"Added metadata columns: database, schema")
        elif source_conn['type'] == 'Excel':
            df['sheet_name'] = source_conn.get('sheet_name')
            log_info(f"Added metadata column: sheet_name")
        elif source_conn['type'] in ['CSV', 'Excel']:
            df['table_name'] = self.target_table
            log_info(f"Added metadata column: table_name")

        df['pipeline_run_timestamp'] = datetime.now()
        log_info(f"Added metadata column: pipeline_run_timestamp")

        log_end_step("Data transformation")
        return df

    def apply_transformation(self, df: pd.DataFrame, transformation: Dict) -> pd.DataFrame:
        transformation_type = transformation['type']
        log_info(f"Applying transformation: {transformation_type}")

        if transformation_type == 'add_timestamp':
            df[transformation['column_name']] = datetime.now()
            log_info(f"Added timestamp to column: {transformation['column_name']}")
        elif transformation_type == 'rename_column':
            df = df.rename(columns={transformation['old_name']: transformation['new_name']})
            log_info(f"Renamed column from {transformation['old_name']} to {transformation['new_name']}")
        elif transformation_type == 'standardize_phone':
            df[transformation['column_name']] = df[transformation['column_name']].apply(self.standardize_phone)
            log_info(f"Standardized phone numbers in column: {transformation['column_name']}")
        elif transformation_type == 'calculate_value':
            df[transformation['new_column']] = df.eval(transformation['formula'])
            log_info(f"Calculated new column: {transformation['new_column']}")
        # Add more transformation types as needed
        return df

    def standardize_phone(self, phone: str) -> str:
        standardized_phone = ''.join(filter(str.isdigit, str(phone)))
        return standardized_phone

class PipelineManager:
    def __init__(self, pipeline_folder: str, schedules_folder: str, connections_folder: str = 'Connections', utilities_folder: str = 'Utilities'):
        self.pipeline_folder = pipeline_folder
        self.schedules_folder = schedules_folder
        self.connections_folder = connections_folder
        self.utilities_folder = utilities_folder
        self.pipelines: Dict[str, Dict] = {}
        self.load_pipelines()

    def load_pipelines(self):
        for filename in os.listdir(self.pipeline_folder):
            if filename.endswith('.yaml'):
                file_path = os.path.join(self.pipeline_folder, filename)
                with open(file_path, 'r') as file:
                    pipeline_config = yaml.safe_load(file)
                    if pipeline_config.get('enabled', True):
                        self.pipelines[pipeline_config['name']] = pipeline_config
                        logging.info(f"Loaded pipeline configuration for {pipeline_config['name']}")

    def run_pipeline(self, pipeline_name: str):
        if pipeline_name in self.pipelines:
            pipeline_config = self.pipelines[pipeline_name]
            pipeline = Pipeline(pipeline_config, self.connections_folder, self.utilities_folder)
            pipeline.run()
        else:
            logging.error(f"Pipeline {pipeline_name} not found")

    def list_pipelines(self) -> List[str]:
        return list(self.pipelines.keys())
    
    def list_schedules(self) -> List[str]:
        return os.listdir(self.schedules_folder)

    def run_schedule(self, schedule_name: str):
        schedule_path = os.path.join(self.schedules_folder, schedule_name)
        if os.path.exists(schedule_path):
            with open(schedule_path, 'r') as file:
                schedule_config = yaml.safe_load(file)
                for pipeline_name in schedule_config.get('pipelines', []):
                    if pipeline_name in self.pipelines:
                        self.run_pipeline(pipeline_name)
                    else:
                        logging.error(f"Pipeline {pipeline_name} in schedule {schedule_name} not found")
        else:
            logging.error(f"Schedule {schedule_name} not found")

    def run_schedules(self):
        for schedule_name in os.listdir(self.schedules_folder):
            self.run_schedule(schedule_name)

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    manager = PipelineManager("Pipelines", "Schedules", "Connections", "Utilities")