#start of transformations.py
import pandas as pd
from typing import Dict, Any
from datetime import datetime
import re
from utilities import logger

def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Data transformation")
        for transformation in self.config.get('transformations', []):
            df = self.apply_transformation(df, transformation)

        if self.config.get('add_metadata', False):
            self.add_metadata_columns(df)
            
        logger.info("Data transformation")
        return df
        
def apply_transformation(self, df: pd.DataFrame, transformation: Dict[str, Any]) -> pd.DataFrame:

    transformation_type = transformation['type']

    logger.info(f"Applying transformation: {transformation_type}")


    if transformation_type == 'add_timestamp':

        df[transformation['column_name']] = datetime.now()

        logger.info(f"Added timestamp to column: {transformation['column_name']}")

    elif transformation_type == 'rename_column':

        df = df.rename(columns={transformation['old_name']: transformation['new_name']})

        logger.info(f"Renamed column from {transformation['old_name']} to {transformation['new_name']}")

    elif transformation_type == 'standardize_phone':

        df[transformation['column_name']] = df[transformation['column_name']].apply(self.standardize_phone)

        logger.info(f"Standardized phone numbers in column: {transformation['column_name']}")

    elif transformation_type == 'calculate_value':

        df[transformation['new_column']] = df.eval(transformation['formula'])

        logger.info(f"Calculated new column: {transformation['new_column']}")

    elif transformation_type == 'clean_column_names':

        df.columns = self.clean_column_names(df.columns)

        logger.info("Cleaned column names")

    elif transformation_type == 'format_column_names':

        format_type = transformation['format_type']

        df.columns = self.format_column_names(df.columns, format_type)

        logger.info(f"Formatted column names to {format_type}")

    return df


def clean_column_names(self, columns: pd.Index) -> pd.Index:

    clean_columns = [re.sub(r'\W+', '_', col).strip('_') for col in columns]

    clean_columns = [re.sub(r'_+', '_', col) for col in clean_columns]

    return pd.Index(clean_columns)


def format_column_names(self, columns: pd.Index, format_type: str) -> pd.Index:

    if format_type == 'camel_case':

        return pd.Index([self.to_camel_case(col) for col in columns])

    elif format_type == 'all_caps':

        return pd.Index([col.upper() for col in columns])

    elif format_type == 'all_lower':

        return pd.Index([col.lower() for col in columns])

    return columns


def to_camel_case(self, s: str) -> str:

    parts = s.split('_')

    return parts[0].lower() + ''.join(word.capitalize() for word in parts[1:])


def standardize_phone(self, phone: str) -> str:

    standardized_phone = ''.join(filter(str.isdigit, str(phone)))

    return standardized_phone


def add_metadata_columns(self, df: pd.DataFrame):

    source_conn = self.config_loader.get_config_by_name(self.source_connection_name)

    df['source_connector_name'] = self.source_connection_name

    df['source_connector_type'] = source_conn['type']

    logger.info(f"Added metadata columns: source_connector_name, source_connector_type")


    if source_conn['type'] == 'MS SQL Server':

        df['database'] = source_conn.get('database')

        df['schema'] = source_conn.get('schema')

        logger.info(f"Added metadata columns: database, schema")

    elif source_conn['type'] == 'Excel':

        df['sheet_name'] = source_conn.get('sheet_name')

        logger.info(f"Added metadata column: sheet_name")

    elif source_conn['type'] in ['CSV', 'Excel']:

        df['table_name'] = self.target_table_name

        logger.info(f"Added metadata column: table_name")


    df['pipeline_run_timestamp'] = datetime.now()

    logger.info(f"Added metadata column: pipeline_run_timestamp")

#end of transformations.py