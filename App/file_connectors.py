#start of file_connectors.py
import pandas as pd

class ExcelHandler:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_data(self, sheet_name=0, start_row=0, start_col=0):
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=None)
        df = df.iloc[start_row:, start_col:]
        return df

    def write_data(self, data, sheet_name='Sheet1', start_row=0, start_col=0, mode='replace'):
        if mode == 'replace':
            with pd.ExcelWriter(self.file_path, engine='openpyxl', mode='w') as writer:
                data.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=start_row, startcol=start_col)
        elif mode == 'append':
            with pd.ExcelWriter(self.file_path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                data.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=start_row, startcol=start_col)

class CSVHandler:
    def __init__(self, file_path):
        self.file_path = file_path

    def read_data(self):
        df = pd.read_csv(self.file_path)
        return df

    def write_data(self, data, mode='replace'):
        if mode == 'replace':
            data.to_csv(self.file_path, index=False)
        elif mode == 'append':
            data.to_csv(self.file_path, mode='a', header=False, index=False)

#end of file_connectors.py