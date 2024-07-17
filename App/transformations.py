import pandas as pd
import re
from typing import Dict, Any, List
from datetime import datetime

def add_timestamp(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    df[column_name] = datetime.now()
    return df

def rename_column(df: pd.DataFrame, old_name: str, new_name: str) -> pd.DataFrame:
    return df.rename(columns={old_name: new_name})

def standardize_phone(phone: str) -> str:
    return ''.join(filter(str.isdigit, str(phone)))

def calculate_value(df: pd.DataFrame, new_column: str, formula: str) -> pd.DataFrame:
    df[new_column] = df.eval(formula)
    return df

def clean_column_names(columns: pd.Index) -> pd.Index:
    clean_columns = [re.sub(r'\W+', '_', col).strip('_') for col in columns]
    clean_columns = [re.sub(r'_+', '_', col) for col in clean_columns]
    return pd.Index(clean_columns)

def format_column_names(columns: pd.Index, format_type: str) -> pd.Index:
    if format_type == 'camel_case':
        return pd.Index([to_camel_case(col) for col in columns])
    elif format_type == 'all_caps':
        return pd.Index([col.upper() for col in columns])
    elif format_type == 'all_lower':
        return pd.Index([col.lower() for col in columns])
    return columns

def to_camel_case(s: str) -> str:
    parts = s.split('_')
    return parts[0].lower() + ''.join(word.capitalize() for word in parts[1:])

def apply_transformation(df: pd.DataFrame, transformation: Dict[str, Any]) -> pd.DataFrame:
    transformation_type = transformation['type']

    if transformation_type == 'add_timestamp':
        return add_timestamp(df, transformation['column_name'])
    elif transformation_type == 'rename_column':
        return rename_column(df, transformation['old_name'], transformation['new_name'])
    elif transformation_type == 'standardize_phone':
        df[transformation['column_name']] = df[transformation['column_name']].apply(standardize_phone)
        return df
    elif transformation_type == 'calculate_value':
        return calculate_value(df, transformation['new_column'], transformation['formula'])
    elif transformation_type == 'clean_column_names':
        df.columns = clean_column_names(df.columns)
        return df
    elif transformation_type == 'format_column_names':
        df.columns = format_column_names(df.columns, transformation['format_type'])
        return df
    else:
        raise ValueError(f"Unsupported transformation type: {transformation_type}")

def transform(df: pd.DataFrame, transformations: List[Dict[str, Any]]) -> pd.DataFrame:
    for transformation in transformations:
        df = apply_transformation(df, transformation)
    return df
