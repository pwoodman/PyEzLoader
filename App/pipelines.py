import os
import pandas as pd
import yaml
import logging
from datetime import datetime
from connectors import DatabaseConnector
from utilities import log_start_step, log_end_step, log_info, log_warning, log_error, load_email_settings, send_email, trigger_dbt_run

class Pipeline:
    def __init__(self, config, connections_folder='Connections', email_folder='Emails'):
        self.config = config
        self.connector = DatabaseConnector(connections_folder)
        self.dbt_run_url = config.get('dbt_run_url')
        self.email_settings = load_email_settings(email_folder, config.get('email_notification'))

    def run(self):
        try:
            log_start_step("Pipeline Run")
            source_connector = self.connector.get_connector(self.config['source']['connection'])
            target_connector = self.connector.get_connector(self.config['target']['connection'])

            source_query = self.config['source'].get('query')
            log_start_step("Read Data from Source")
            source_df = source_query and source_connector.read(source_query) or source_connector.read()
            log_end_step("Read Data from Source")

            target_table = self.config['target']['table_name']
            log_start_step("Check Target Table")
            if not target_connector.table_exists(target_table):
                log_warning(f"Target table {target_table} does not exist. It will be created when writing data.")
                target_df = pd.DataFrame()
            else:
                target_df = target_connector.read(f"SELECT * FROM {target_table}")
            log_end_step("Check Target Table")

            if not target_df.empty and source_df.shape[1] != target_df.shape[1]:
                log_info("Schema mismatch detected, truncating target table.")
                target_connector.truncate_table(target_table)

            log_start_step("Transform Data")
            transformed_df = self.transform(source_df)
            transformed_df['pipeline_run_timestamp'] = datetime.now()
            log_end_step("Transform Data")

            log_start_step("Write Data to Target")
            target_connector.write(transformed_df, target_table, self.config['target']['mode'])
            log_end_step("Write Data to Target")

            log_end_step("Pipeline Run")
            trigger_dbt_run(self.dbt_run_url)
        except Exception as e:
            log_error(f"Pipeline run failed: {e}")
            send_email(self.email_settings, "Pipeline Failure Alert", f"Pipeline run failed with error: {e}")

    def transform(self, df):
        log_start_step("Data Transformation")
        for transformation in self.config.get('transformations', []):
            if transformation['type'] == 'add_timestamp':
                df[transformation['column_name']] = datetime.now()
                log_info(f"Added timestamp to column: {transformation['column_name']}")

        source_conn = self.connector.connections[self.config['source']['connection']]
        df['source_connector_name'] = self.config['source']['connection']
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
            df['table_name'] = self.config['target']['table_name']
            log_info(f"Added metadata column: table_name")

        log_end_step("Data Transformation")
        return df

class PipelineManager:
    def __init__(self, pipeline_folder, schedules_folder, connections_folder='Connections', email_folder='Emails'):
        self.pipeline_folder = pipeline_folder
        self.schedules_folder = schedules_folder
        self.connections_folder = connections_folder
        self.email_folder = email_folder
        self.pipelines = {}
        self.load_pipelines()

    def load_pipelines(self):
        for filename in os.listdir(self.pipeline_folder):
            if filename.endswith('.yaml'):
                with open(os.path.join(self.pipeline_folder, filename), 'r') as file:
                    config = yaml.safe_load(file)
                    for pipeline_config in config.get('pipelines', []):
                        if pipeline_config.get('enabled', True):
                            self.pipelines[pipeline_config['name']] = pipeline_config
                            logging.info(f"Loaded pipeline configuration for {pipeline_config['name']}.")

    def run_pipeline(self, pipeline_name):
        pipeline_config = self.pipelines.get(pipeline_name)
        if not pipeline_config:
            logging.error(f"Pipeline not found: {pipeline_name}")
            return

        logging.info(f"Running pipeline: {pipeline_name}")
        pipeline = Pipeline(pipeline_config, self.connections_folder, self.email_folder)
        pipeline.run()
