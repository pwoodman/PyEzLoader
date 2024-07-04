import io
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.pool import QueuePool
import chardet
import os
import yaml

class BaseConnector:
    def __init__(self, config):
        self.config = config
        self.transaction_mode = config.get('transaction_mode', 'read_write')

    def _connect(self):
        raise NotImplementedError("_connect method not implemented")

    def _start_transaction(self, conn):
        if self.transaction_mode in ('write', 'read_write'):
            conn.execute("START TRANSACTION")
            logging.info("Transaction started.")

    def _commit_transaction(self, conn):
        if self.transaction_mode in ('write', 'read_write'):
            conn.execute("COMMIT")
            logging.info("Transaction committed.")

    def _rollback_transaction(self, conn):
        if self.transaction_mode in ('write', 'read_write'):
            conn.execute("ROLLBACK")
            logging.warning("Transaction rolled back.")

    def read(self, query=None):
        conn = self._connect()
        self._start_transaction(conn)
        try:
            df = pd.read_sql(query, conn) if query else pd.DataFrame()
            self._commit_transaction(conn)
        except Exception as e:
            self._rollback_transaction(conn)
            logging.error(f"Error during read operation: {e}")
            raise
        finally:
            conn.close()
        logging.info("Successfully read data from the database.")
        return df

    def write(self, df, table_name, mode='append'):
        conn = self._connect()
        self._start_transaction(conn)
        try:
            if mode == 'truncate':
                self.truncate_table(table_name)
            df.to_sql(table_name, conn, if_exists='append', index=False)
            self._commit_transaction(conn)
        except Exception as e:
            self._rollback_transaction(conn)
            logging.error(f"Error during write operation: {e}")
            raise
        finally:
            conn.close()
        logging.info(f"Successfully wrote data to {table_name} table.")

    def truncate_table(self, table_name):
        conn = self._connect()
        self._start_transaction(conn)
        try:
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            self._commit_transaction(conn)
        except Exception as e:
            self._rollback_transaction(conn)
            logging.error(f"Error during truncate operation: {e}")
            raise
        finally:
            conn.close()
        logging.info(f"Successfully truncated {table_name} table.")

class ExcelConnector(BaseConnector):
    def read(self, query=None):
        df = pd.read_excel(self.config['file_path'],
                           sheet_name=self.config.get('sheet_name'),
                           header=self.config.get('header_start_row', 0))
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
        df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, engine='python', on_bad_lines='warn')
        logging.info(f"Successfully read data from file: {file_path} with encoding {encoding}.")
        return df

    def write(self, df, table_name, mode='append'):
        file_path = self.config['file_path']
        encoding = self.config.get('encoding', 'utf-8')
        delimiter = '\t' if file_path.lower().endswith('.tsv') else ','
        df.to_csv(file_path, index=False, sep=delimiter, encoding=encoding)
        logging.info(f"Successfully wrote data to file: {file_path} with encoding {encoding}.")

class PostgreSQLConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.engine = self._create_engine()

    def _create_engine(self):
        connection_string = (
            f"postgresql+psycopg2://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config.get('port', 5432)}/"
            f"{self.config.get('dbname', 'postgres')}"
        )
        return create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800,
            connect_args={'sslmode': 'require'}
        )

    def _connect(self):
        return self.engine.connect()

    def table_exists(self, table_name):
        with self.engine.connect() as connection:
            result = connection.execute(text(f"SELECT to_regclass('{table_name}') IS NOT NULL")).scalar()
            return result if result is not None else False

    def read(self, query):
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)
            logging.info(f"Successfully executed query: {query}")
            return df
        except ProgrammingError as e:
            logging.error(f"Error executing query: {e}")
            raise

    def write(self, df, table_name, mode='append'):
        if_exists = 'replace' if mode == 'truncate' else 'append'
        with self.engine.connect() as connection:
            if if_exists == 'replace':
                connection.execute(text(f"DROP TABLE IF EXISTS \"{table_name}\""))
                df.head(0).to_sql(table_name, connection, if_exists='replace', index=False)

            # Disable indexes
            connection.execute(text(f"ALTER TABLE \"{table_name}\" DISABLE TRIGGER ALL"))

            output = io.StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)

            # Use copy_expert to execute the COPY command
            copy_sql = f"COPY \"{table_name}\" FROM STDIN WITH CSV DELIMITER E'\t'"
            connection.connection.connection.cursor().copy_expert(sql=copy_sql, file=output)

            # Re-enable indexes
            connection.execute(text(f"ALTER TABLE \"{table_name}\" ENABLE TRIGGER ALL"))
            connection.commit()
        logging.info(f"Successfully wrote data to {table_name} table.")

    def truncate_table(self, table_name):
        if self.table_exists(table_name):
            with self.engine.connect() as connection:
                connection.execute(text(f"TRUNCATE TABLE \"{table_name}\""))
            logging.info(f"Successfully truncated {table_name} table.")
        else:
            logging.warning(f"Table {table_name} does not exist. Skipping truncate operation.")

    def __del__(self):
        if hasattr(self, 'engine'):
            self.engine.dispose()

class MySQLConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.engine = self._create_engine()

    def _create_engine(self):
        connection_string = (
            f"mysql+mysqlconnector://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config.get('port', 3306)}/"
            f"{self.config['dbname']}"
        )
        return create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800
        )

    def _connect(self):
        return self.engine.connect()

    def read(self, query):
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)
            logging.info(f"Successfully executed query: {query}")
            return df
        except ProgrammingError as e:
            logging.error(f"Error executing query: {e}")
            raise

    def write(self, df, table_name, mode='append'):
        if_exists = 'replace' if mode == 'truncate' else 'append'
        with self.engine.connect() as connection:
            df.to_sql(table_name, connection, if_exists=if_exists, index=False)
        logging.info(f"Successfully wrote data to {table_name} table.")

    def truncate_table(self, table_name):
        if self.table_exists(table_name):
            with self.engine.connect() as connection:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            logging.info(f"Successfully truncated {table_name} table.")
        else:
            logging.warning(f"Table {table_name} does not exist. Skipping truncate operation.")

    def table_exists(self, table_name):
        with self.engine.connect() as connection:
            result = connection.execute(text(f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")).scalar()
            return result if result is not None else False

    def __del__(self):
        if hasattr(self, 'engine'):
            self.engine.dispose()

class RedshiftConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.engine = self._create_engine()

    def _create_engine(self):
        connection_string = (
            f"redshift+psycopg2://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config.get('port', 5439)}/"
            f"{self.config['dbname']}"
        )
        return create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=1800
        )

    def _connect(self):
        return self.engine.connect()

    def read(self, query):
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)
            logging.info(f"Successfully executed query: {query}")
            return df
        except ProgrammingError as e:
            logging.error(f"Error executing query: {e}")
            raise

    def write(self, df, table_name, mode='append'):
        if_exists = 'replace' if mode == 'truncate' else 'append'
        with self.engine.connect() as connection:
            df.to_sql(table_name, connection, if_exists=if_exists, index=False)
        logging.info(f"Successfully wrote data to {table_name} table.")

    def truncate_table(self, table_name):
        if self.table_exists(table_name):
            with self.engine.connect() as connection:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            logging.info(f"Successfully truncated {table_name} table.")
        else:
            logging.warning(f"Table {table_name} does not exist. Skipping truncate operation.")

    def table_exists(self, table_name):
        with self.engine.connect() as connection:
            result = connection.execute(text(f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'")).scalar()
            return result if result is not None else False

    def __del__(self):
        if hasattr(self, 'engine'):
            self.engine.dispose()

class DatabaseConnector:
    CONNECTOR_MAP = {
        'Excel': ExcelConnector,
        'CSV': CSVConnector,
        'PostgreSQL': PostgreSQLConnector,
        'MySQL': MySQLConnector,
        'Redshift': RedshiftConnector
    }

    def __init__(self, connections_folder='Connections'):
        self.connections = {}
        self.load_connections(connections_folder)

    def load_connections(self, folder):
        for filename in os.listdir(folder):
            if filename.endswith('.yaml'):
                with open(os.path.join(folder, filename), 'r') as file:
                    config = yaml.safe_load(file)
                    self.connections[config['ConnectionName']] = config
                    logging.info(f"Loaded connection configuration for {config['ConnectionName']}.")

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
