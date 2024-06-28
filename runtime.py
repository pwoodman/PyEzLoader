import pandas as pd
import pyodbc
import snowflake.connector
import yaml

class DatabaseConnector:
    def __init__(self, yaml_file):
        with open(yaml_file, 'r') as file:
            self.config = yaml.safe_load(file)
    
    def read(self, connection_name):
        connection = self.config['connections'][connection_name]
        if connection['type'] == 'Excel':
            return self.read_excel(connection['file_path'], connection.get('sheet_name'), connection.get('range'), connection.get('header_start_row'))
        elif connection['type'] == 'CSV':
            return self.read_csv(connection['file_path'])
        elif connection['type'] == 'MS SQL Server':
            return self.read_sql_server(connection['server'], connection['database'], connection['authentication']['username'], connection['authentication']['password'])
        elif connection['type'] == 'Snowflake':
            return self.read_snowflake(connection['account'], connection['database'], connection['schema'], connection['authentication']['username'], connection['authentication']['password'])
    
    def write(self, connection_name, df):
        connection = self.config['connections'][connection_name]
        if connection['type'] == 'Excel':
            self.write_excel(df, connection['file_path'])
        elif connection['type'] == 'CSV':
            self.write_csv(df, connection['file_path'])
        elif connection['type'] == 'MS SQL Server':
            self.write_sql_server(df, connection['server'], connection['database'], connection['authentication']['username'], connection['authentication']['password'])
        elif connection['type'] == 'Snowflake':
            self.write_snowflake(df, connection['account'], connection['database'], connection['schema'], connection['authentication']['username'], connection['authentication']['password'])
    
    def read_excel(self, file_path, sheet_name=None, range=None, header_start_row=0):
        if sheet_name:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_start_row)
        else:
            df = pd.read_excel(file_path, header=header_start_row)
        
        if range:
            df = df.loc[range]
        
        return df

    def write_excel(self, df, file_path):
        df.to_excel(file_path, index=False)

    def read_csv(self, file_path):
        df = pd.read_csv(file_path)
        return df

    def write_csv(self, df, file_path):
        df.to_csv(file_path, index=False)

    def read_sql_server(self, server, database, username, password):
        conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
        cursor = conn.cursor()
        query = "SELECT * FROM my_table"
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        return df

    def write_sql_server(self, df, server, database, username, password):
        conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
        cursor = conn.cursor()
        for index, row in df.iterrows():
            values = tuple(row)
            cursor.execute("INSERT INTO my_table VALUES (?, ?)", values)
        conn.commit()
        cursor.close()
        conn.close()

    def read_snowflake(self, account, database, schema, username, password):
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            database=database,
            schema=schema
        )
        cursor = conn.cursor()
        query = "SELECT * FROM my_table"
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall())
        cursor.close()
        conn.close()
        return df

    def write_snowflake(self, df, account, database, schema, username, password):
        conn = snowflake.connector.connect(
            user=username,
            password=password,
            account=account,
            database=database,
            schema=schema
        )
        cursor = conn.cursor()
        for index, row in df.iterrows():
            values = tuple(row)
            cursor.execute("INSERT INTO my_table VALUES (?, ?)", values)
        conn.commit()
        cursor.close()
        conn.close()
