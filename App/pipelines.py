import os
import pandas as pd
import yaml
from datetime import datetime
from typing import Dict, List, Any
import logging
import argparse

from utilities import (
    log_start_step, log_end_step, log_info, log_warning, log_error,
    UtilityLoader, run_dbt_if_configured, send_email_if_configured
)

from connectors import DatabaseConnector

class ConfigLoader:
    def __init__(self, connections_folder: str):
        self.connections_folder = connections_folder
        self.connections = self.load_connections()
        log_info(f"Loaded connections: {self.connections.keys()}")

    def load_connections(self) -> Dict[str, Any]:
        connections = {}
        log_info(f"Loading connections from folder: {self.connections_folder}")
        
        if not os.path.exists(self.connections_folder):
            log_error(f"Connections folder does not exist: {self.connections_folder}")
            return connections

        folder_contents = os.listdir(self.connections_folder)
        log_info(f"Contents of {self.connections_folder}: {folder_contents}")

        for filename in folder_contents:
            file_path = os.path.join(self.connections_folder, filename)
            log_info(f"Checking file: {file_path}")
            
            if filename.endswith('.yaml'):
                try:
                    with open(file_path, 'r') as file:
                        file_content = file.read()
                        log_info(f"File content of {filename}:\n{file_content}")
                        connection_config = yaml.safe_load(file_content)
                        log_info(f"Loaded connection config: {connection_config}")
                        if 'name' not in connection_config:
                            log_error(f"Connection configuration in {filename} is missing 'name' key")
                            continue
                        connections[connection_config['name']] = connection_config
                except yaml.YAMLError as e:
                    log_error(f"Error parsing YAML file {file_path}: {e}")
                except Exception as e:
                    log_error(f"Error loading connection config {file_path}: {e}")
            else:
                log_info(f"Skipping non-YAML file: {filename}")

        log_info(f"Completed loading connections. Total connections loaded: {len(connections)}")
        return connections

    def get_config_by_name(self, connector_name: str) -> Dict[str, Any]:
        if connector_name not in self.connections:
            log_error(f"Connection {connector_name} not found. Available connections: {list(self.connections.keys())}")
            raise ValueError(f"Connection {connector_name} not found")
        return self.connections[connector_name]

class Pipeline:
    def __init__(self, config: Dict[str, Any], connections_folder: str, utilities_folder: str):
        self.config = config
        self.validate_config()
        self.config_loader = ConfigLoader(connections_folder)
        self.utility_loader = UtilityLoader(utilities_folder)
        
        self.setup_source()
        self.setup_target()

    def validate_config(self):
        required_keys = ['name', 'source', 'target', 'transformations']
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration key: {key}")

    def setup_source(self):
        source_config = self.config['source']
        self.source_connection_name = source_config['connection_name']
        log_info(f"Setting up source connection: {self.source_connection_name}")

    def setup_target(self):
        target_config = self.config['target']
        self.target_connection_name = target_config['connection_name']
        self.target_action = target_config['action']
        self.target_schema_name = target_config.get('schema_name')
        self.target_table_name = target_config['table_name']
        log_info(f"Setting up target connection: {self.target_connection_name}, action: {self.target_action}, schema: {self.target_schema_name}, table: {self.target_table_name}")

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
            run_dbt_if_configured(self.config, self.utility_loader)
        except Exception as e:
            self.handle_error("Pipeline run failed", e)

    def read_source(self) -> pd.DataFrame:
        log_start_step("Reading source data")
        try:
            source_config = self.config_loader.get_config_by_name(self.source_connection_name)
        except ValueError as e:
            log_error(f"Source connection error: {e}")
            raise
        
        source_connector = DatabaseConnector.get_connector(source_config['type'], source_config)
        df = source_connector.read()
        log_end_step("Reading source data")
        return df

    def write_target(self, df: pd.DataFrame):
        log_start_step("Writing target data")
        try:
            target_config = self.config_loader.get_config_by_name(self.target_connection_name)
        except ValueError as e:
            log_error(f"Target connection error: {e}")
            raise

        target_connector = DatabaseConnector.get_connector(target_config['type'], target_config)
        action = self.target_action
        schema_name = self.target_schema_name
        table_name = self.target_table_name

        if action == 'append':
            target_connector.write(df, table_name, mode='append', schema=schema_name)
        elif action == 'truncate_and_load':
            target_connector.truncate_table(table_name, schema=schema_name)
            target_connector.write(df, table_name, mode='append', schema=schema_name)
        elif action == 'drop_and_load':
            target_connector.drop_table(table_name, schema=schema_name)
            target_connector.write(df, table_name, mode='replace', schema=schema_name)
        else:
            raise ValueError(f"Unsupported action: {action}")

        log_end_step("Writing target data")

    def handle_error(self, error_type: str, error: Exception):
        log_error(f"{error_type}: {error}")
        send_email_if_configured(self.config, self.utility_loader, f"Pipeline Failure Alert - {error_type}", f"{error_type} in pipeline {self.config['name']}: {error}")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        log_start_step("Data transformation")
        for transformation in self.config.get('transformations', []):
            df = self.apply_transformation(df, transformation)

        self.add_metadata_columns(df)
        log_end_step("Data transformation")
        return df

    def apply_transformation(self, df: pd.DataFrame, transformation: Dict[str, Any]) -> pd.DataFrame:
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
        return df

    def standardize_phone(self, phone: str) -> str:
        standardized_phone = ''.join(filter(str.isdigit, str(phone)))
        return standardized_phone

    def add_metadata_columns(self, df: pd.DataFrame):
        source_conn = self.config_loader.get_config_by_name(self.source_connection_name)
        df['source_connector_name'] = self.source_connection_name
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
            df['table_name'] = self.target_table_name
            log_info(f"Added metadata column: table_name")

        df['pipeline_run_timestamp'] = datetime.now()
        log_info(f"Added metadata column: pipeline_run_timestamp")

class PipelineManager:
    def __init__(self, pipeline_folder: str, schedules_folder: str, connections_folder: str, utilities_folder: str):
        self.pipeline_folder = pipeline_folder
        self.schedules_folder = schedules_folder
        self.connections_folder = connections_folder
        self.utilities_folder = utilities_folder
        self.pipelines: Dict[str, Dict[str, Any]] = {}
        self.load_pipelines()

    def load_pipelines(self):
        for filename in os.listdir(self.pipeline_folder):
            if filename.endswith('.yaml'):
                file_path = os.path.join(self.pipeline_folder, filename)
                try:
                    with open(file_path, 'r') as file:
                        pipeline_config = yaml.safe_load(file)
                        if 'name' not in pipeline_config:
                            log_error(f"Pipeline configuration in {filename} is missing 'name' key")
                            continue
                        if pipeline_config.get('enabled', True):
                            self.pipelines[pipeline_config['name']] = pipeline_config
                            log_info(f"Loaded pipeline configuration for {pipeline_config['name']}")
                except yaml.YAMLError as e:
                    log_error(f"Error parsing YAML file {file_path}: {e}")
                except Exception as e:
                    log_error(f"Error loading pipeline config {file_path}: {e}")

    def run_pipeline(self, pipeline_name: str):
        if pipeline_name in self.pipelines:
            pipeline_config = self.pipelines[pipeline_name]
            pipeline = Pipeline(pipeline_config, self.connections_folder, self.utilities_folder)
            pipeline.run()
        else:
            log_error(f"Pipeline {pipeline_name} not found")

    def list_pipelines(self) -> List[str]:
        return list(self.pipelines.keys())
    
    def list_schedules(self) -> List[str]:
        return os.listdir(self.schedules_folder)

    def run_schedule(self, schedule_name: str):
        schedule_path = os.path.join(self.schedules_folder, schedule_name)
        if os.path.exists(schedule_path):
            try:
                with open(schedule_path, 'r') as file:
                    schedule_config = yaml.safe_load(file)
                    for pipeline_name in schedule_config.get('pipelines', []):
                        if pipeline_name in self.pipelines:
                            self.run_pipeline(pipeline_name)
                        else:
                            log_error(f"Pipeline {pipeline_name} in schedule {schedule_name} not found")
            except yaml.YAMLError as e:
                log_error(f"Error parsing YAML file {schedule_path}: {e}")
            except Exception as e:
                log_error(f"Error loading schedule config {schedule_path}: {e}")
        else:
            log_error(f"Schedule {schedule_name} not found")

    def run_schedules(self):
        for schedule_name in os.listdir(self.schedules_folder):
            self.run_schedule(schedule_name)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description="Run a specified data pipeline.")
    parser.add_argument("pipeline_name", type=str, help="Name of the pipeline to run")

    args = parser.parse_args()
    pipeline_name = args.pipeline_name

    current_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(current_dir)
    pipelines_folder = os.path.join(base_dir, 'Pipelines')
    connections_folder = os.path.join(base_dir, 'Connections')
    schedules_folder = os.path.join(base_dir, 'Schedules')
    utilities_folder = os.path.join(base_dir, 'Utilities')

    log_info(f"Current directory: {current_dir}")
    log_info(f"Base directory: {base_dir}")
    log_info(f"Pipelines folder: {pipelines_folder}")
    log_info(f"Connections folder: {connections_folder}")
    log_info(f"Schedules folder: {schedules_folder}")
    log_info(f"Utilities folder: {utilities_folder}")

    for folder in [pipelines_folder, connections_folder, schedules_folder, utilities_folder]:
        if os.path.exists(folder):
            log_info(f"{os.path.basename(folder)} folder exists. Contents:")
            for file in os.listdir(folder):
                file_path = os.path.join(folder, file)
                if file.endswith('.yaml'):
                    try:
                        with open(file_path, 'r') as yaml_file:
                            log_info(f"  YAML file found: {file}")
                            log_info(f"  Contents of {file}:\n{yaml_file.read()}")
                    except Exception as e:
                        log_error(f"  Error reading {file}: {str(e)}")
                else:
                    log_info(f"  Non-YAML file: {file}")
        else:
            log_error(f"{os.path.basename(folder)} folder does not exist: {folder}")

    try:
        manager = PipelineManager(pipelines_folder, schedules_folder, connections_folder, utilities_folder)
        manager.run_pipeline(pipeline_name)
    except Exception as e:
        log_error(f"Error running pipeline {pipeline_name}: {str(e)}")
        raise

    log_info("Script execution completed.")
