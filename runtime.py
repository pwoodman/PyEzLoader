import os
import pandas as pd
import yaml
import chardet
import logging
import argparse
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import ProgrammingError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseConnector:
    def __init__(self, config):
        self.config = config

    def _connect(self):
        raise NotImplementedError("_connect method not implemented")

    def read(self, query):
        conn = self._connect()
        df = pd.read_sql(query, conn)
        conn.close()
        logging.info("Successfully read data from the database.")
        return df

    def write(self, df, table_name, mode='append'):
        conn = self._connect()
        cursor = conn.cursor()
        if mode == 'truncate':
            self.truncate_table(table_name)
        placeholder = '%s' if self.config['type'] in ['PostgreSQL', 'MySQL'] else '?'
        for _, row in df.iterrows():
            cursor.execute(f"INSERT INTO {table_name} VALUES ({', '.join([placeholder for _ in row])})", tuple(row))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Successfully wrote data to {table_name} table.")

    def truncate_table(self, table_name):
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Successfully truncated {table_name} table.")

class ExcelConnector(BaseConnector):
    def read(self, query=None):
        df = pd.read_excel(self.config['file_path'], 
                           sheet_name=self.config.get('sheet_name'), 
                           header=self.config.get('header_start_row', 0))
        if 'range' in self.config:
            df = df.loc[self.config['range']]
        logging.info(f"Successfully read data from Excel file: {self.config['file_path']}.")
        return df

    def write(self, df, table_name, mode='append'):
        df.to_excel(self.config['file_path'], index=False)
        logging.info(f"Successfully wrote data to Excel file: {self.config['file_path']}.")
        
class CSVConnector(BaseConnector):
    def read(self, query=None):
        file_path = self.config['file_path']
        encoding = self.config.get('encoding')
        if not encoding:
            with open(file_path, 'rb') as f:
                rawdata = f.read(10000)
                result = chardet.detect(rawdata)
                encoding = result['encoding']
        delimiter = '\t' if file_path.lower().endswith('.tsv') else ','
        fallback_encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        for enc in [encoding] + fallback_encodings:
            try:
                df = pd.read_csv(file_path, encoding=enc, delimiter=delimiter, engine='python', on_bad_lines='warn')
                logging.info(f"Successfully read data from file: {file_path} with encoding {enc}.")
                return df
            except UnicodeDecodeError as e:
                logging.warning(f"Failed to read file {file_path} with encoding {enc}. Error: {e}")
            except pd.errors.ParserError as e:
                logging.warning(f"Parser error encountered while reading file {file_path} with encoding {enc}. Error: {e}")
        logging.error(f"Failed to read file {file_path} with all attempted encodings.")
        raise UnicodeDecodeError(f"Failed to read file {file_path} with all attempted encodings.")
        
    def write(self, df, table_name, mode='append'):
        file_path = self.config['file_path']
        encoding = self.config.get('encoding', 'utf-8')
        delimiter = '\t' if file_path.lower().endswith('.tsv') else ','
        try:
            df.to_csv(file_path, index=False, sep=delimiter, encoding=encoding)
            logging.info(f"Successfully wrote data to file: {file_path} with encoding {encoding}.")
        except Exception as e:
            logging.error(f"Failed to write data to file: {file_path}. Error: {e}")
            raise

class PostgreSQLConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.engine = self._create_engine()

    def _create_engine(self):
        try:
            connection_string = (
                f"postgresql+psycopg2://{self.config['user']}:{self.config['password']}@"
                f"{self.config['host']}:{self.config.get('port', 5432)}/"
                f"{self.config.get('dbname', 'postgres')}"
            )
            return create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                connect_args={'sslmode': 'require'}
            )
        except Exception as e:
            error_msg = f"Failed to create database engine: {e}"
            logging.error(error_msg)
            raise ConnectionError(error_msg)

    def table_exists(self, table_name):
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(
                    f"SELECT to_regclass('{table_name}') IS NOT NULL"
                ))
                return result.scalar()
        except Exception as e:
            logging.error(f"Error checking if table exists: {e}")
            return False

    def read(self, query):
        if query is None:
            logging.warning("No query provided for PostgreSQL read operation. Returning empty DataFrame.")
            return pd.DataFrame()
        
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)
            logging.info(f"Successfully executed query: {query}")
            return df
        except ProgrammingError as e:
            if 'relation' in str(e) and 'does not exist' in str(e):
                table_name = str(e).split('"')[1]
                logging.warning(f"Table '{table_name}' does not exist. Returning empty DataFrame.")
                return pd.DataFrame()
            else:
                raise
        except Exception as e:
            error_msg = f"Error executing query: {e}"
            logging.error(error_msg)
            raise

    def write(self, df, table_name, mode='append'):
        try:
            # Convert all object columns to strings
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str)
            
            if_exists = 'replace' if mode == 'truncate' else 'append'
            
            with self.engine.connect() as connection:
                df.to_sql(table_name, connection, if_exists=if_exists, index=False)
            
            logging.info(f"Successfully wrote data to {table_name} table.")
        except Exception as e:
            error_msg = f"Error writing data to {table_name}: {e}"
            logging.error(error_msg)
            raise

    def truncate_table(self, table_name):
        try:
            if self.table_exists(table_name):
                with self.engine.connect() as connection:
                    connection.execute(text(f"TRUNCATE TABLE {table_name}"))
                logging.info(f"Successfully truncated {table_name} table.")
            else:
                logging.warning(f"Table {table_name} does not exist. Skipping truncate operation.")
        except Exception as e:
            error_msg = f"Error truncating table {table_name}: {e}"
            logging.error(error_msg)
            raise

    def __del__(self):
        if hasattr(self, 'engine'):
            self.engine.dispose()

class DatabaseConnector:
    CONNECTOR_MAP = {
        'Excel': ExcelConnector,
        'CSV': CSVConnector,
        'PostgreSQL': PostgreSQLConnector
    }

    def __init__(self, connections_folder='Connections'):
        self.connections = {}
        self.load_connections(connections_folder)

    def load_connections(self, folder):
        for filename in os.listdir(folder):
            if filename.endswith('.yaml'):
                try:
                    with open(os.path.join(folder, filename), 'r') as file:
                        config = yaml.safe_load(file)
                        self.connections[config['ConnectionName']] = config
                        logging.info(f"Loaded connection configuration for {config['ConnectionName']}.")
                except yaml.YAMLError as e:
                    logging.error(f"Failed to load YAML configuration: {e}")

    def get_connector(self, connection_name):
        config = self.connections.get(connection_name)
        if not config:
            logging.error(f"Connection '{connection_name}' not found")
            raise ValueError(f"Connection '{connection_name}' not found")
        connector_class = self.CONNECTOR_MAP.get(config['type'])
        if not connector_class:
            logging.error(f"Unsupported connection type: {config['type']}")
            raise ValueError(f"Unsupported connection type: {config['type']}")
        return connector_class(config)

class Pipeline:
    def __init__(self, config, connections_folder='Connections'):
        self.config = config
        self.connector = DatabaseConnector(connections_folder)

    def run(self):
        source_query = self.config['source'].get('query')
        source_connector = self.connector.get_connector(self.config['source']['connection'])
        target_connector = self.connector.get_connector(self.config['target']['connection'])
        
        if source_query:
            source_df = source_connector.read(source_query)
        else:
            source_df = source_connector.read()

        target_table = self.config['target']['table_name']
        
        if not target_connector.table_exists(target_table):
            logging.warning(f"Target table {target_table} does not exist. It will be created when writing data.")
            target_df = pd.DataFrame()
        else:
            target_df = target_connector.read(f"SELECT * FROM {target_table}")

        if not target_df.empty and source_df.shape[1] != target_df.shape[1]:
            logging.info("Schema mismatch detected, truncating target table.")
            target_connector.truncate_table(target_table)

        transformed_df = self.transform(source_df)
        transformed_df['pipeline_run_timestamp'] = datetime.now()
        target_connector.write(transformed_df, target_table, self.config['target']['mode'])
        logging.info("Pipeline run completed successfully.")

    def transform(self, df):
        for transformation in self.config.get('transformations', []):
            if transformation['type'] == 'add_timestamp':
                df[transformation['column_name']] = datetime.now()
        source_conn = self.connector.connections[self.config['source']['connection']]
        df['source_connector_name'] = self.config['source']['connection']
        df['source_connector_type'] = source_conn['type']
        if source_conn['type'] == 'MS SQL Server':
            df['database'] = source_conn.get('database')
            df['schema'] = source_conn.get('schema')
        elif source_conn['type'] == 'Excel':
            df['sheet_name'] = source_conn.get('sheet_name')
        elif source_conn['type'] in ['CSV', 'Excel']:
            df['table_name'] = self.config['target']['table_name']
        return df

class PipelineManager:
    def __init__(self, pipeline_folder, schedules_folder, connections_folder='Connections'):
        self.pipeline_folder = pipeline_folder
        self.schedules_folder = schedules_folder
        self.connections_folder = connections_folder
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
        pipeline = Pipeline(pipeline_config, self.connections_folder)
        pipeline.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run data pipelines.")
    parser.add_argument("--run", help="Run a specific pipeline by name")
    args = parser.parse_args()

    pipeline_manager = PipelineManager('Pipelines', 'Schedules', 'Connections')

    if args.run:
        pipeline_manager.run_pipeline(args.run)
    else:
        logging.info("No pipeline specified to run. Use --run <pipeline_name> to run a specific pipeline.")
