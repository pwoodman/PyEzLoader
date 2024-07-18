import os
import pandas as pd
import yaml
from typing import Dict, List, Any, Optional, Callable
import argparse
from datetime import datetime, timedelta
from utilities import logger
from transformations import transform
from database_connectors import process_request, get_yaml_value, DatabaseError
from file_connectors import ExcelHandler, CSVHandler

class ConfigLoader:
    def __init__(self, connections_folder: str):
        self.connections_folder = connections_folder
        self.connections = self.load_connections()
        logger.info(f"Loaded connections: {self.connections.keys()}")

    def load_connections(self) -> Dict[str, Any]:
        connections = {}
        logger.info(f"Loading connections from folder: {self.connections_folder}")

        if not os.path.exists(self.connections_folder):
            logger.error(f"Connections folder does not exist: {self.connections_folder}")
            return connections

        for filename in os.listdir(self.connections_folder):
            if filename.endswith('.yaml'):
                file_path = os.path.join(self.connections_folder, filename)
                try:
                    with open(file_path, 'r') as file:
                        connection_config = yaml.safe_load(file)
                        if 'name' not in connection_config:
                            logger.error(f"Connection configuration in {filename} is missing 'name' key")
                            continue
                        connections[connection_config['name']] = connection_config
                        logger.info(f"Loaded connection config: {connection_config['name']}")
                except Exception as e:
                    logger.error(f"Error loading connection config {file_path}: {e}")

        return connections

    def get_config_by_name(self, connector_name: str) -> Dict[str, Any]:
        if connector_name not in self.connections:
            logger.error(f"Connection {connector_name} not found. Available connections: {list(self.connections.keys())}")
            raise ValueError(f"Connection {connector_name} not found")
        return self.connections[connector_name]

class Pipeline:
    def __init__(self, config: Dict[str, Any], connections_folder: str, utilities_folder: str):
        self.config = config
        self.connections_folder = connections_folder
        self.utilities_folder = utilities_folder
        self.validate_config()
        self.config_loader = ConfigLoader(connections_folder)
        
        self.setup_source()
        self.setup_target()
        self.status = {
            "success": True,
            "errors": [],
            "source_rows": 0,
            "target_rows": 0,
        }
        self.timings = {
            "start_time": None,
            "end_time": None,
            "read_source": {"start": None, "end": None, "duration": None},
            "transform": {"start": None, "end": None, "duration": None},
            "write_target": {"start": None, "end": None, "duration": None},
        }
        self.source_data = None
        self.transformed_data = None

    def validate_config(self):
        required_keys = ['name', 'source', 'target', 'transformations']
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration key: {key}")

    def setup_source(self):
        source_config = self.config['source']
        self.source_connection_name = source_config['connection_name']
        self.source_query = source_config.get('query')
        logger.info(f"Setting up source connection: {self.source_connection_name}")

    def setup_target(self):
        target_config = self.config['target']
        self.target_connection_name = target_config['connection_name']
        self.target_action = target_config['action']
        self.target_schema_name = target_config.get('schema_name')
        
        connector_type = self.config_loader.get_config_by_name(self.target_connection_name)['type']
        if connector_type not in ['CSV', 'Excel']:
            if 'table_name' not in target_config:
                raise ValueError("Missing required configuration key: 'table_name'")
            self.target_table_name = target_config['table_name']
        else:
            self.target_table_name = None
            self.sheet_name = target_config.get('sheet_name')
            self.header_start_row = target_config.get('header_start_row', 0)
            self.column_start_row = target_config.get('column_start_row', 'A')

        logger.info(f"Setting up target connection: {self.target_connection_name}, action: {self.target_action}, schema: {self.target_schema_name}, table: {self.target_table_name}")

    def run(self):
        self.timings["start_time"] = datetime.now()
        try:
            logger.info(f"Pipeline run: {self.config['name']}")
            
            self.source_data = self.time_operation("read_source", self.read_source)
            self.status["source_rows"] = len(self.source_data)
            logger.info(f"Data read from source. Rows: {self.status['source_rows']}")

            self.transformed_data = self.time_operation("transform", self.transform_data)
            logger.info("Data transformed")

            if not self.transformed_data.empty:
                # Define columns and types based on the transformed data
                columns = {col: 'TEXT' for col in self.transformed_data.columns}  # Adjust types as necessary
                self.time_operation("write_target", self.write_target, self.transformed_data, columns)
            
            if self.status["success"]:
                logger.info(f"Pipeline run completed successfully: {self.config['name']}")
            else:
                logger.error(f"Pipeline run failed: {self.config['name']}")
        except Exception as e:
            self.handle_error("Pipeline run failed", e)
        finally:
            self.timings["end_time"] = datetime.now()
            self.log_summary()

    def time_operation(self, operation_name: str, operation_func: Callable, *args, **kwargs):
        self.timings[operation_name]["start"] = datetime.now()
        result = operation_func(*args, **kwargs)
        self.timings[operation_name]["end"] = datetime.now()
        self.timings[operation_name]["duration"] = self.timings[operation_name]["end"] - self.timings[operation_name]["start"]
        return result

    def transform_data(self):
        return transform(self.source_data, self.config.get('transformations', []))

    def write_target(self, df: pd.DataFrame, columns: Dict[str, str]) -> None:
        logger.info("Writing target data")
        try:
            target_config = self.config_loader.get_config_by_name(self.target_connection_name)
            target_connector_type = target_config['type']
            
            if target_connector_type == 'CSV':
                target_connector = CSVHandler(file_path=target_config['file_path'], encoding=target_config.get('encoding', 'utf-8'), delimiter=target_config.get('delimiter', ','))
            elif target_connector_type == 'Excel':
                target_connector = ExcelHandler(file_path=target_config['file_path'], sheet_name=target_config.get('sheet_name', 'Sheet1'), header_start_row=target_config.get('header_start_row', 0), column_start_row=target_config.get('column_start_row', 'A'))
            else:
                if self.target_action == 'append':
                    method = 'append'
                elif self.target_action == 'truncate_and_load':
                    method = 'truncate_and_load'
                elif self.target_action == 'drop_and_load':
                    method = 'drop_and_load'
                else:
                    raise ValueError(f"Unsupported action: {self.target_action}")

                process_request(
                    username=target_config['username'],
                    password=target_config['password'],
                    server=target_config['server'],
                    database=target_config['database'],
                    schema=self.target_schema_name,
                    table_name=self.target_table_name,
                    port=target_config.get('port'),
                    db_type=target_connector_type,
                    method=method,
                    df=df
                )

            if target_connector_type in ['CSV', 'Excel']:
                if self.target_action == 'replace':
                    mode = 'replace'
                elif self.target_action == 'append':
                    mode = 'append'
                else:
                    raise ValueError(f"Unsupported action for {target_connector_type}: {self.target_action}")
                target_connector.write_data(df, mode=mode)

            self.status["target_rows"] = len(df)
            logger.info(f"Target data written successfully. Rows: {self.status['target_rows']}")
        except DatabaseError as e:
            self.handle_error("Database error writing target data", e)
        except Exception as e:
            self.handle_error("Error writing target data", e)
            self.status["target_rows"] = 0

    def log_summary(self):
        total_duration = self.timings["end_time"] - self.timings["start_time"]
        summary = [
            f"\n{'='*50}",
            f"Pipeline Summary for {self.config['name']}:",
            f"{'='*50}",
            f"Status: {'Success' if self.status['success'] else 'Failed'}",
            f"Source Rows: {self.status['source_rows']}",
            f"Target Rows: {self.status['target_rows']} {'(Failed to write)' if not self.status['success'] else ''}",
            f"Total Duration: {total_duration}",
            f"{'-'*50}",
            "Step Timings:",
            f"Read Source: {self.timings['read_source']['duration']}",
            f"Transform: {self.timings['transform']['duration']}",
            f"Write Target: {self.timings['write_target']['duration']}",
            f"{'-'*50}"
        ]
        
        if self.status["errors"]:
            summary.extend([
                "Errors:",
                f"{'-'*50}"
            ])
            for i, error in enumerate(self.status["errors"], 1):
                summary.extend([
                    f"Error {i}:",
                    f"{error}",
                    f"{'-'*50}"
                ])
        
        summary.append(f"{'='*50}\n")
        
        logger.info("\n".join(summary))

    def read_source(self) -> pd.DataFrame:
        logger.info("Reading source data")
        try:
            source_config = self.config_loader.get_config_by_name(self.source_connection_name)
            source_connector_type = source_config['type']
            
            if source_connector_type == 'CSV':
                source_connector = CSVHandler(file_path=source_config['file_path'], encoding=source_config.get('encoding', 'utf-8'), delimiter=source_config.get('delimiter', ','))
                df = source_connector.read_data()
            elif source_connector_type == 'Excel':
                source_connector = ExcelHandler(file_path=source_config['file_path'], sheet_name=source_config.get('sheet_name', 'Sheet1'), header_start_row=source_config.get('header_start_row', 0), column_start_row=source_config.get('column_start_row', 'A'))
                df = source_connector.read_data()
            else:
                df = process_request(
                    username=source_config['username'],
                    password=source_config['password'],
                    server=source_config['server'],
                    database=source_config['database'],
                    port=source_config.get('port'),
                    db_type=source_connector_type,
                    method='read',
                    query=self.source_query or f"SELECT * FROM {source_config['table']}"
                )

            logger.info(f"Source data read successfully. Rows: {len(df)}")
            return df
        except DatabaseError as e:
            self.handle_error("Database error reading source data", e)
        except Exception as e:
            self.handle_error("Error reading source data", e)
        return pd.DataFrame()

    def handle_error(self, error_type: str, error: Exception):
        self.status["success"] = False
        error_message = f"{error_type}: {str(error)}"
        self.status["errors"].append(error_message)
        logger.error(error_message)

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
                            logger.error(f"Pipeline configuration in {filename} is missing 'name' key")
                            continue
                        if pipeline_config.get('enabled', True):
                            self.pipelines[pipeline_config['name']] = pipeline_config
                            logger.info(f"Loaded pipeline configuration for {pipeline_config['name']}")
                except yaml.YAMLError as e:
                    logger.error(f"Error parsing YAML file {file_path}: {e}")
                except Exception as e:
                    logger.error(f"Error loading pipeline config {file_path}: {e}")

    def run_pipeline(self, pipeline_name: str):
        if pipeline_name in self.pipelines:
            pipeline_config = self.pipelines[pipeline_name]
            pipeline = Pipeline(pipeline_config, self.connections_folder, self.utilities_folder)
            pipeline.run()
        else:
            logger.error(f"Pipeline {pipeline_name} not found")

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
                            logger.error(f"Pipeline {pipeline_name} in schedule {schedule_name} not found")
            except yaml.YAMLError as e:
                logger.error(f"Error parsing YAML file {schedule_path}: {e}")
            except Exception as e:
                logger.error(f"Error loading schedule config {schedule_path}: {e}")
        else:
            logger.error(f"Schedule {schedule_name} not found")

    def run_schedules(self):
        for schedule_name in os.listdir(self.schedules_folder):
            self.run_schedule(schedule_name)

if __name__ == "__main__":
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

    logger.info(f"Current directory: {current_dir}")
    logger.info(f"Base directory: {base_dir}")
    logger.info(f"Pipelines folder: {pipelines_folder}")
    logger.info(f"Connections folder: {connections_folder}")
    logger.info(f"Schedules folder: {schedules_folder}")
    logger.info(f"Utilities folder: {utilities_folder}")

    try:
        manager = PipelineManager(pipelines_folder, schedules_folder, connections_folder, utilities_folder)
        manager.run_pipeline(pipeline_name)
    except Exception as e:
        logger.error(f"Error running pipeline {pipeline_name}: {str(e)}")
        raise

    logger.info("Script execution completed.")