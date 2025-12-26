

import pyodbc
import os
import pandas as pd
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from dotenv import load_dotenv , find_dotenv # to load .env files
# from google.cloud import storage

# client = storage.Client()
load_dotenv()


warehouse_servr = os.environ.get("DB1_HOST")
warehouse_user = os.environ.get("DB1_USER")
warehouse_pass = os.environ.get("DB1_PASS")
warehouse_db = os.environ.get("DB1_NAME") 

conn = pyodbc.connect(
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={warehouse_servr};"  # Warehouse server
            f"Database={warehouse_db};"  # Warehouse database
            f"UID={warehouse_user};"
            f"PWD={warehouse_pass};"
        )

warehouse_cursor = conn.cursor()

warehouse_cursor.fast_executemany = True

# Path to your downloaded key
service_acc_key = os.getenv("gcp_bot_key")

# Define the required scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authorize
creds = Credentials.from_service_account_file(service_acc_key, scopes=SCOPES)
gc = gspread.authorize(creds)


def bulk_insert(table_name, data, batch_size):
    try:
        columns = data.columns.tolist()
        # columns_str = ', '.join(columns)
        columns_str = ', '.join([f'[{col}]' for col in columns])
        placeholders = ', '.join(['?' for _ in range(len(columns))])
        insert_query = f"INSERT INTO WAVE..{table_name} ({columns_str}) VALUES ({placeholders})"
        
        total_inserted = 0
        
        # Process in batches
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i: i + batch_size]
            records = [tuple(row) for _, row in batch.iterrows()]
            
            print(f"Executing batch insert for records {i} to {i + len(records) - 1}")
            warehouse_cursor.executemany(insert_query, records)
            
            total_inserted += len(records) 
            print(f"Inserted batch: {total_inserted}/{len(data)} records")
                
        
        print(f"✓ Successfully inserted all {total_inserted} records into {table_name}")
        conn.commit()
        # conn.close()
        
    except Exception as e:
        # conn.rollback()
        print(f"✗ Error inserting data: {str(e)}")
        
    # finally:
    #     warehouse_cursor.close()
    #     conn.close()

wave_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1SKgKmXeInkKkhEB4eIj0bEBMXel-fvYEnqe4n7OE1hc/edit?usp=sharing')

wave_sheet_1 = wave_sheet.worksheet('Sheet1')

wave_sheet_df = pd.DataFrame(wave_sheet_1.get_all_records())

warehouse_cursor.execute("SELECT * FROM WAVE..[Wave_Progress_2]")

rows_tuple = [tuple(i) for i in warehouse_cursor.fetchall()]
column = [column[0] for column in warehouse_cursor.description]
wave_progress_df = pd.DataFrame(rows_tuple, columns=column)

prod_server = os.environ.get("DB2_HOST")
prod_user = os.environ.get("DB2_USER")     
prod_pass = os.environ.get("DB2_PASS")
prod_db = os.environ.get("DB2_NAME")

conn_prod = pyodbc.connect(
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={prod_server};"
            f"Database={prod_db};"
            f"UID={prod_user};"
            f"PWD={prod_pass};"
        )
prod_cursor = conn_prod.cursor()
prod_cursor.fast_executemany = True

prod_cursor.execute('''
SELECT bl.block_code , bl.block_name , d.district_code , d.district_name , 
tr.territory_name, st.state_code, st.state_name
FROM [Drishtee_stats_new2024].[dbo].[tbl_block_master] bl
LEFT JOIN [Drishtee_stats_new2024].[dbo].[tbl_district_master] d ON d.district_code = bl.district_code
LEFT JOIN [Drishtee_stats_new2024].[dbo].tbl_territory_master tr ON d.territory_code = tr.territory_code
LEFT JOIN [Drishtee_stats_new2024].[dbo].[tbl_state_master] st ON st.state_code = d.state_code
                    ''')
rows_tuple = [tuple(i) for i in prod_cursor.fetchall()]
column = [column[0] for column in prod_cursor.description]
block_df = pd.DataFrame(rows_tuple, columns=column)


wave_block_df = pd.merge(wave_sheet_df , 
    block_df, left_on=['State', 'Cluster', 'Block'], right_on=['state_name', 'district_name', 'block_name'], how='left'
)

wave_block_df = wave_block_df[['Wave', 'Update', 'State', 'Territory', 'Cluster', 'Block','block_code',
       'Block Status', 'Vatika', 'Employee Name', 'Activity', 'Target_Date',
       'Remarks', 'Identify', 'Expected Date', 'Complete_Date']]

wave_block_df.rename(columns={
    'block_code' : 'BlockId',
    'Block Status' : 'Block_Status',
    'Employee Name' : 'Employee_Name',
    'Expected Date' : 'Expected_Date',
}, inplace=True)

wave_block_df['Update'] = pd.to_datetime(wave_block_df['Update'])
wave_block_df['Target_Date'] = pd.to_datetime(wave_block_df['Target_Date'], errors='coerce')
wave_block_df['Expected_Date'] = pd.to_datetime(wave_block_df['Expected_Date'],errors='coerce')
wave_block_df['Complete_Date'] = pd.to_datetime(wave_block_df['Complete_Date'], errors='coerce')
wave_block_df['BlockId'] = pd.to_numeric(wave_block_df['BlockId'], errors='coerce').fillna(0).astype('int')

wave_block_df['Expected_Date'] = wave_block_df['Expected_Date'].fillna(pd.Timestamp('1999-01-01'))
wave_block_df['Complete_Date'] = wave_block_df['Complete_Date'].fillna(pd.Timestamp('1999-01-01'))
wave_block_df['Target_Date'] = wave_block_df['Target_Date'].fillna(pd.Timestamp('1999-01-01'))
wave_block_df['Update'] = wave_block_df['Target_Date'].fillna(pd.Timestamp('1999-01-01'))

wave_block_df['Block_Key'] = wave_block_df['State'] + ' ' + wave_block_df['Cluster'] + ' ' + wave_block_df['Block']
wave_block_df['insert_on'] = pd.to_datetime(datetime.now().date())

backup = warehouse_cursor.execute('''
                        USE WAVE
                                  
                        TRUNCATE TABLE  Wave_backup
                                  
                        INSERT INTO Wave_backup
                        SELECT [Wave]
                            ,[Update]
                            ,[State]
                            ,[Territory]
                            ,[Cluster]
                            ,[Block]
                            ,[BlockId]
                            ,[Block_Status]
                            ,[Vatika]
                            ,[Employee_Name]
                            ,[Activity]
                            ,[Target_Date]
                            ,[Remarks]
                            ,[Identify]
                            ,[Expected_Date]
                            ,[Complete_Date]
                            ,[Block_Key] 
                            FROM Wave_Progress_2
                                  
                        TRUNCATE TABLE  Wave_Progress_2
                         ''')
conn.commit()

if backup:
    print("Wave_Progress_2 Backup Created Successfully...!")

    table_name = "Wave_Progress_2"  
data = wave_block_df
batch_size = 1000   
bulk_insert(table_name,data, batch_size)