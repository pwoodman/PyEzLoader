import yaml
import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from time import sleep
from typing import Optional, Dict, Any
from utilities import logger

class DatabaseError(Exception):
    """Custom exception for database errors."""
    pass

def get_yaml_value(yaml_file: str = 'C:/Users/patri/OneDrive/Documents/Patrick Learning/PyEzLoader/Connections/Dev_MSSQL_Test.yaml', key: str = 'your_key') -> Any:
    """Retrieve a value from a YAML file."""
    try:
        with open(yaml_file, 'r') as file:
            return yaml.safe_load(file)[key]
    except Exception as e:
        logger.error(f"Error reading YAML file: {str(e)}")
        raise DatabaseError(f"Error reading YAML file: {str(e)}")

def create_sql_engine(username: str, password: str, server: str, database: str, port: Optional[int] = None, db_type: Optional[str] = None) -> Engine:
    """Create a SQLAlchemy engine for database connection."""
    logger.info(f"Connecting to database: {database}")
    if db_type == 'mssql':
        driver = "ODBC+Driver+17+for+SQL+Server"
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}{':{port}' if port else ''}/{database}?driver={driver}"
    elif db_type == 'postgresql':
        conn_str = f"postgresql://{username}:{password}@{server}{f':{port}' if port else ''}/{database}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    try:
        if db_type == 'mssql':
            engine = create_engine(conn_str, fast_executemany=True)
        else:
            engine = create_engine(conn_str,executemany_mode='batch')
        engine.connect()  # Test the connection
        logger.info(f"Successfully connected to database: {database}")
        return engine
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise DatabaseError(f"Error connecting to database: {str(e)}")

def read_data(engine: Engine, select_query: str) -> pd.DataFrame:
    """Read data from the database using a SELECT query."""
    logger.info(f"Reading data from database using query: {select_query}")
    try:
        query = text(select_query)
        with engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        logger.info(f"Data read from database: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error reading data from database: {str(e)}")
        raise DatabaseError(f"Error reading data from database: {str(e)}")

def write_data(engine: Engine, df: pd.DataFrame, schema: str, table_name: str) -> None:
    """Write data to the database using efficient bulk insert."""
    logger.info(f"Writing data to database: {schema}.{table_name}")
    try:
        rows_to_add = len(df)
        if rows_to_add > 0:
            df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False, chunksize=1000)
        
        rows_after_write = engine.execute(text(f"SELECT COUNT(1) FROM {schema}.{table_name}")).scalar()
        logger.info(f"Successfully wrote {rows_to_add} rows to database: {schema}.{table_name}")
        logger.info(f"Total rows in table after write: {rows_after_write}")
    except SQLAlchemyError as e:
        logger.error(f"SQLAlchemy error writing data to database: {str(e)}")
        raise DatabaseError(f"Error writing data to database: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error writing data to database: {str(e)}")
        raise DatabaseError(f"Error writing data to database: {str(e)}")

def close_connection(engine: Engine) -> None:
    """Close the database connection."""
    engine.dispose()
    logger.info("Database connection closed")

def truncate_table(engine: Engine, schema: str, table_name: str) -> None:
    """Truncate a table in the database."""
    logger.info(f"Truncating table: {schema}.{table_name}")
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
        sleep(5)
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(1) FROM {schema}.{table_name}"))
            rows = result.scalar()
        if rows == 0:
            logger.info(f"Table {schema}.{table_name} truncated successfully")
        else:
            logger.error(f"Error truncating table: {schema}.{table_name}")
            raise DatabaseError(f"Error truncating table: {schema}.{table_name}")
    except Exception as e:
        logger.error(f"Error truncating table: {str(e)}")
        raise DatabaseError(f"Error truncating table: {str(e)}")

def drop_table(engine: Engine, schema: str, table_name: str) -> None:
    """Drop a table from the database."""
    logger.info(f"Dropping table: {schema}.{table_name}")
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {schema}.{table_name}"))
        logger.info(f"Table {schema}.{table_name} dropped successfully")
    except Exception as e:
        logger.error(f"Error dropping table: {str(e)}")
        raise DatabaseError(f"Error dropping table: {str(e)}")

def process_request(username: str, password: str, server: str, schema: Optional[str] = None, 
                    table_name: Optional[str] = None, port: Optional[int] = None, 
                    database: Optional[str] = None, query: Optional[str] = None, 
                    db_type: str = 'mssql', method: str = 'append', df: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]:
    """
    Process a database request based on the specified method.
    """
    engine = None
    try:
        if not all([username, password, server, database]):
            raise ValueError("Missing required connection parameters")
        
        engine = create_sql_engine(username, password, server, database, port=port, db_type=db_type)
        
        if method == 'read':
            if not query:
                raise ValueError("Query is required for 'read' method")
            return read_data(engine, query)
        elif method in ['append', 'truncate_and_load', 'drop_and_load']:
            if not all([schema, table_name, df is not None]):
                raise ValueError("Schema, table_name, and DataFrame are required for write operations")
            
            # Check if the table exists, if not, create it
            inspector = inspect(engine)
            if not inspector.has_table(table_name, schema=schema):
                logger.info(f"Table {schema}.{table_name} does not exist. Creating it.")
                df.head(0).to_sql(table_name, engine, schema=schema, if_exists='fail', index=False)
            
            if method == 'truncate_and_load':
                truncate_table(engine, schema, table_name)
            elif method == 'drop_and_load':
                drop_table(engine, schema, table_name)
                df.head(0).to_sql(table_name, engine, schema=schema, if_exists='fail', index=False)
            
            write_data(engine, df, schema, table_name)
        else:
            raise ValueError(f"Unsupported method: {method}")
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        raise DatabaseError(f"Error processing request: {str(e)}")
    finally:
        if engine:
            close_connection(engine)