import os
import pandas as pd
import pyodbc
import snowflake.connector
import yaml
import schedule
import time
from datetime import datetime, timedelta
from croniter import croniter
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseConnector:
    def __init__(self, config):
        self.config = config

    def read(self, query=None):
        raise NotImplementedError("Read method not implemented")

    def write(self, df, table_name, mode='append'):
        raise NotImplementedError("Write method not implemented")

    def truncate_table(self, table_name):
        raise NotImplementedError("Truncate method not implemented")

class ExcelConnector(BaseConnector):
    def read(self, query=None):
        df = pd.read_excel(self.config['file_path'], 
                           sheet_name=self.config.get('sheet_name'), 
                           header=self.config.get('header_start_row', 0))
        if 'range' in self.config:
            df = df.loc[self.config['range']]
        return df

    def write(self, df, table_name, mode='append'):
        df.to_excel(self.config['file_path'], index=False)

class CSVConnector(BaseConnector):
    def read(self, query=None):
        return pd.read_csv(self.config['file_path'])

    def write(self, df, table_name, mode='append'):
        df.to_csv(self.config['file_path'], index=False)

class MSSQLConnector(BaseConnector):
    def _connect(self):
        return pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={self.config["server"]};'
            f'UID={self.config["username"]};'
            f'PWD={self.config["password"]}'
        )

    def read(self, query):
        conn = self._connect()
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def write(self, df, table_name, mode='append'):
        conn = self._connect()
        cursor = conn.cursor()
        if mode == 'truncate':
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        for _, row in df.iterrows():
            cursor.execute(f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in row])})", tuple(row))
        conn.commit()
        cursor.close()
        conn.close()

    def truncate_table(self, table_name):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        cursor.close()
        conn.close()

class SnowflakeConnector(BaseConnector):
    def _connect(self):
        return snowflake.connector.connect(
            user=self.config['username'],
            password=self.config['password'],
            account=self.config['account']
        )

    def read(self, query):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        cursor.close()
        conn.close()
        return df

    def write(self, df, table_name, mode='append'):
        conn = self._connect()
        cursor = conn.cursor()
        if mode == 'truncate':
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        for _, row in df.iterrows():
            cursor.execute(f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in row])})", tuple(row))
        conn.commit()
        cursor.close()
        conn.close()

    def truncate_table(self, table_name):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        cursor.close()
        conn.close()

class DatabaseConnector:
    def __init__(self, connector_config_paths):
        self.connections = {}
        for path in connector_config_paths:
            with open(path, 'r') as file:
                config = yaml.safe_load(file)
                connection_name = config['ConnectionName']
                self.connections[connection_name] = config

    def get_connector(self, connection_name):
        connection = self.connections.get(connection_name)
        if not connection:
            raise ValueError(f"Connection '{connection_name}' not found")
        
        connector_class = {
            'Excel': ExcelConnector,
            'CSV': CSVConnector,
            'MS SQL Server': MSSQLConnector,
            'Snowflake': SnowflakeConnector
        }.get(connection['type'])

        if not connector_class:
            raise ValueError(f"Unsupported connection type: {connection['type']}")
        
        return connector_class(connection)

class Pipeline:
    def __init__(self, config):
        self.config = config
        self.connector = DatabaseConnector(config['connector_config_paths'])

    def run(self):
        source_query = self.config['source']['query']
        if not source_query:
            raise ValueError("No source query provided in pipeline configuration")
        
        source_connector = self.connector.get_connector(self.config['source']['connection'])
        target_connector = self.connector.get_connector(self.config['target']['connection'])

        source_df = source_connector.read(source_query)
        target_df = target_connector.read(f"SELECT * FROM {self.config['target']['table_name']}")

        if not target_df.empty and source_df.shape[1] != target_df.shape[1]:
            logging.info("Schema mismatch detected, truncating target table.")
            target_connector.truncate_table(self.config['target']['table_name'])

        transformed_df = self.transform(source_df)
        transformed_df['pipeline_run_timestamp'] = datetime.now()
        target_connector.write(transformed_df, self.config['target']['table_name'], self.config['target']['mode'])

    def transform(self, df):
        for transformation in self.config['transformations']:
            if transformation['type'] == 'add_timestamp':
                df[transformation['column_name']] = datetime.now()
        
        df['source_connector_name'] = self.config['source']['connection']
        df['source_connector_type'] = self.connector.connections[self.config['source']['connection']]['type']

        source_conn = self.connector.connections[self.config['source']['connection']]
        if source_conn['type'] == 'MS SQL Server':
            df['database'] = source_conn.get('database')
            df['schema'] = source_conn.get('schema')
        elif source_conn['type'] == 'Excel':
            df['sheet_name'] = source_conn.get('sheet_name')
        elif source_conn['type'] in ['CSV', 'Excel']:
            df['table_name'] = self.config['target']['table_name']
        
        return df

class PipelineManager:
    def __init__(self, pipeline_folder, schedules_file):
        self.pipeline_folder = pipeline_folder
        self.schedules_file = schedules_file
        self.pipelines = []
        self.schedules = self.load_schedules()

    def load_schedules(self):
        with open(self.schedules_file, 'r') as file:
            return yaml.safe_load(file)['schedules']

    def load_pipelines(self):
        for filename in os.listdir(self.pipeline_folder):
            if filename.endswith('.yaml'):
                with open(os.path.join(self.pipeline_folder, filename), 'r') as file:
                    config = yaml.safe_load(file)
                    if config.get('enabled', True):
                        self.pipelines.append(Pipeline(config))

    def schedule_pipelines(self):
        for pipeline in self.pipelines:
            schedule_name = pipeline.config['schedule']['name']
            schedule_config = self.schedules.get(schedule_name)
            if not schedule_config:
                raise ValueError(f"Schedule '{schedule_name}' not found in schedules file")

            schedule_type = schedule_config['schedule_type']
            if schedule_type == 'daily':
                time_str = schedule_config['time']
                schedule.every().day.at(time_str).do(pipeline.run)
            elif schedule_type == 'hourly':
                minute = schedule_config['minute']
                schedule.every().hour.at(f":{minute:02d}").do(pipeline.run)
            elif schedule_type == 'cron':
                cron_expression = schedule_config['cron']
                days_offset = schedule_config.get('days_offset', 0)
                self.schedule_cron(pipeline, cron_expression, days_offset)
            else:
                raise ValueError(f"Unsupported schedule type: {schedule_type}")

    def schedule_cron(self, pipeline, cron_expression, days_offset):
        def job():
            cron_iter = croniter(cron_expression, datetime.now())
            next_run = cron_iter.get_next(datetime) + timedelta(days=days_offset)
            schedule.every().day.at(next_run.strftime('%H:%M')).do(pipeline.run)
        job()

    def run(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    pipeline_manager = PipelineManager('Pipelines', 'schedules.yaml')
    pipeline_manager.load_pipelines()
    pipeline_manager.schedule_pipelines()
    pipeline_manager.run()
