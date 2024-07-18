import pandas as pd
from openpyxl import load_workbook
import os
from typing import Any
from utilities import logger

class ExcelHandler:
    def __init__(self, file_path: str, sheet_name: str = 'Sheet1', header_start_row: int = 0, column_start_row: Any = 'A'):
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.header_start_row = header_start_row
        self.column_start_row = self._ensure_column_is_string(column_start_row)
        self._validate_file_path(file_path)

    def _ensure_column_is_string(self, column):
        if isinstance(column, int):
            return chr(ord('A') + column)
        elif isinstance(column, str):
            return column.upper()
        else:
            raise ValueError("column_start_row must be either an integer or a string")

    def _validate_file_path(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    def read_data(self) -> pd.DataFrame:
        try:
            column_start_index = self._convert_column_to_index(self.column_start_row)
            df = pd.read_excel(self.file_path, sheet_name=self.sheet_name, header=None)
            df = df.iloc[self.header_start_row:, column_start_index:]
            
            # Generate column names if they don't exist
            if df.columns.dtype != 'object':
                df.columns = [f'Column_{i+1}' for i in range(len(df.columns))]
            
            logger.info(f"Data read from Excel file: {self.file_path}")
            logger.debug(f"DataFrame shape: {df.shape}")
            logger.debug(f"DataFrame columns: {df.columns}")
            return df
        except Exception as e:
            logger.error(f"Error reading data from Excel file: {str(e)}")
            raise IOError(f"Error reading data from Excel file: {str(e)}")

    def write_data(self, data: pd.DataFrame, mode: str = 'replace'):
        try:
            if mode == 'replace':
                with pd.ExcelWriter(self.file_path, engine='openpyxl', mode='w') as writer:
                    data.to_excel(writer, sheet_name=self.sheet_name, index=False, header=True, startrow=self.header_start_row, startcol=self._convert_column_to_index(self.column_start_row))
            elif mode == 'append':
                with pd.ExcelWriter(self.file_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                    data.to_excel(writer, sheet_name=self.sheet_name, index=False, header=False, startrow=self.header_start_row, startcol=self._convert_column_to_index(self.column_start_row))
            logger.info(f"Data written to Excel file: {self.file_path}")
            logger.debug(f"Data shape: {data.shape}")
        except Exception as e:
            logger.error(f"Error writing data to Excel file: {str(e)}")
            raise IOError(f"Error writing data to Excel file: {str(e)}")

    def _convert_column_to_index(self, column: str) -> int:
        column = column.upper()
        index = 0
        for i, char in enumerate(reversed(column)):
            index += (ord(char) - ord('A') + 1) * (26 ** i)
        return index - 1

class CSVHandler:
    def __init__(self, file_path: str, encoding: str = 'utf-8', delimiter: str = ','):
        self.file_path = file_path
        self.encoding = encoding
        self.delimiter = delimiter
        self._validate_file_path(file_path)

    def _validate_file_path(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

    def read_data(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.file_path, encoding=self.encoding, delimiter=self.delimiter)
            logger.info(f"Data read from CSV file: {self.file_path}")
            logger.debug(f"DataFrame shape: {df.shape}")
            logger.debug(f"DataFrame columns: {df.columns}")
            return df
        except Exception as e:
            logger.error(f"Error reading data from CSV file: {str(e)}")
            raise IOError(f"Error reading data from CSV file: {str(e)}")

    def write_data(self, data: pd.DataFrame, mode: str = 'replace'):
        try:
            if mode == 'replace':
                data.to_csv(self.file_path, index=False, encoding=self.encoding, sep=self.delimiter)
            elif mode == 'append':
                data.to_csv(self.file_path, mode='a', header=False, index=False, encoding=self.encoding, sep=self.delimiter)
            logger.info(f"Data written to CSV file: {self.file_path}")
            logger.debug(f"Data shape: {data.shape}")
        except Exception as e:
            logger.error(f"Error writing data to CSV file: {str(e)}")
            raise IOError(f"Error writing data to CSV file: {str(e)}")