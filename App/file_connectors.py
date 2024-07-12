from base_connectors import BaseConnector
import pandas as pd
import logging
from typing import Optional
import chardet

logger = logging.getLogger(__name__)

class FileConnector(BaseConnector):
    def read(self, query: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError("read method not implemented")

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append') -> None:
        raise NotImplementedError("write method not implemented")

class ExcelConnector(FileConnector):
    def read(self, query: Optional[str] = None) -> pd.DataFrame:
        try:
            df = pd.read_excel(
                self.config['file_path'],
                sheet_name=self.config.get('sheet_name'),
                header=self.config.get('header_start_row', 0)
            )
            logger.info(f"Successfully read data from Excel file: {self.config['file_path']}.")
            return df
        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise

    def write(self, df: pd.DataFrame, table_name: str = None, mode: str = 'replace',
              sheet_name: Optional[str] = None, start_column: Optional[str] = None, start_row: Optional[int] = None) -> None:
        try:
            file_path = self.config['file_path']
            sheet_name = sheet_name or self.config.get('sheet_name', 'Sheet1')
            start_column = start_column or 'A'
            start_row = start_row or 0

            start_col_index = ord(start_column.upper()) - ord('A')

            if mode == 'replace':
                df.to_excel(file_path, sheet_name=sheet_name, index=False)
                logger.info(f"Successfully replaced data in Excel file: {file_path}, sheet: {sheet_name}.")
            else:
                with pd.ExcelWriter(file_path, engine='openpyxl', mode='a' if mode == 'append' else 'w') as writer:
                    if mode == 'append':
                        workbook = writer.book
                        if sheet_name in workbook.sheetnames:
                            sheet = workbook[sheet_name]
                            for r_idx, row in df.iterrows():
                                for c_idx, value in enumerate(row):
                                    sheet.cell(row=start_row + r_idx + 1, column=start_col_index + c_idx + 1, value=value)
                        else:
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                    else:
                        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=start_row, startcol=start_col_index)
                logger.info(f"Successfully wrote data to Excel file: {file_path}, sheet: {sheet_name}.")
        except Exception as e:
            logger.error(f"Error writing to Excel file: {e}")
            raise

class CSVConnector(FileConnector):
    def read(self, query: Optional[str] = None) -> pd.DataFrame:
        try:
            file_path = self.config['file_path']
            encoding = self.config.get('encoding', 'utf-8')
            delimiter = self.config.get('delimiter', ',')
            if isinstance(delimiter, str) and delimiter.startswith('"') and delimiter.endswith('"'):
                delimiter = delimiter[1:-1]
            df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, engine='python', on_bad_lines='warn')
            logger.info(f"Successfully read data from file: {file_path} with encoding {encoding} and delimiter '{delimiter}'.")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise

    def write(self, df: pd.DataFrame, table_name: str, mode: str = 'append') -> None:
        try:
            file_path = self.config['file_path']
            encoding = self.config.get('encoding', 'utf-8')
            delimiter = self.config.get('delimiter', ',')
            if isinstance(delimiter, str) and delimiter.startswith('"') and delimiter.endswith('"'):
                delimiter = delimiter[1:-1]
            
            if mode == 'replace':
                df.to_csv(file_path, index=False, sep=delimiter, encoding=encoding)
                logger.info(f"Successfully replaced data in CSV file: {file_path}.")
            else:
                df.to_csv(file_path, mode='a', index=False, header=not pd.io.common.file_exists(file_path), sep=delimiter, encoding=encoding)
                logger.info(f"Successfully appended data to CSV file: {file_path}.")
        except Exception as e:
            logger.error(f"Error writing to CSV file: {e}")
            raise

    def _detect_encoding(self, file_path: str) -> str:
        with open(file_path, 'rb') as f:
            rawdata = f.read(10000)
        return chardet.detect(rawdata)['encoding']
