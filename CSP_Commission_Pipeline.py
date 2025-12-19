
import pyodbc
import os
import pandas as pd
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from dotenv import load_dotenv , find_dotenv # to load .env files
load_dotenv()

# Warehouse connection
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

# insert function
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

# downloaded GCP key
service_acc_key = os.environ.get("gcp_bot_key")

# Define the required scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authorize
creds = Credentials.from_service_account_file(service_acc_key, scopes=SCOPES)
gc = gspread.authorize(creds)

# Fetching Sheet Data
csp_comm_sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1r7u4fBEp5gU-4wtcQayBGQynJ4onWA76mM62eQA-7Rc/edit?usp=sharing')

csp_comm_sheet.worksheets()
comm_sheet_df = pd.DataFrame(csp_comm_sheet.worksheet('Sheet1').get_all_records())


# Transformation
comm_sheet_df['Comm_Month'] = pd.to_datetime(comm_sheet_df['Comm_Month'])
comm_sheet_df['Accounting_Month'] = pd.to_datetime(comm_sheet_df['Accounting_Month'])
comm_sheet_df['Last Update'] = pd.to_datetime(comm_sheet_df['Last Update'])
comm_sheet_df['Comm'] = pd.to_numeric(comm_sheet_df['Comm'], errors='coerce').fillna(0).astype('float')
comm_sheet_df['Count'] = pd.to_numeric(comm_sheet_df['Count'], errors='coerce').fillna(0).astype('int')


# tmp_csp_commission
warehouse_cursor.execute("SELECT TOP(10) * FROM WAVE..tmp_csp_commission ORDER BY id DESC")
rows_tuple = [tuple(i) for i in warehouse_cursor.fetchall()]
column = [column[0] for column in warehouse_cursor.description]
tmp_csp_commission_df = pd.DataFrame(rows_tuple, columns=column)


# CSP Logs
warehouse_cursor.execute("SELECT * FROM WAVE..COMMISSION_Update_log")
rows_tuple = [tuple(i) for i in warehouse_cursor.fetchall()]
column = [column[0] for column in warehouse_cursor.description]
commission_log_df = pd.DataFrame(rows_tuple, columns=column)

# Selecting Updated Data to be Inserted
commission_log_df = pd.DataFrame([{
    "last_update_date": comm_sheet_df['Last Update'].max(),"table_name": "CSP_Commission"}])

mx_1 = commission_log_df[commission_log_df['table_name'] == 'CSP_Commission']

comm_sheet_df_1 = comm_sheet_df[comm_sheet_df['Last Update'] > mx_1['last_update_date'].max()]


# Adding New Columns
comm_sheet_df_1['imported_by'] = 'DF1002'
comm_sheet_df_1['imported_at'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) 
comm_sheet_df_1['is_processed'] = 0
comm_sheet_df_1['error_in_process'] = 0


# Selecting Columns for tmp_csp_commission
comm_sheet_df_1 = comm_sheet_df_1[['Comm_Month', 'Accounting_Month', 'Bank', 'CSP_Code', 'State',
       'Territorry', 'District', 'Comm_Pena', 'Revenue_Category',
       'Revenue_Head', 'Count', 'Comm', 'Tag', 'imported_by', 'imported_at',
       'is_processed', 'error_in_process']]


# Inserting Data into tmp_csp_commission
table_name = "tmp_csp_commission"  
batch_size = 1000
idf = comm_sheet_df_1

bulk_insert(table_name, idf, batch_size)


# running procedure to insert into final table
insert_date = datetime.now().strftime('%Y-%m-%d')
warehouse_cursor.execute(f'''
                         USE WAVE;
                         exec sp_Insert_into_CSP_Commission '{insert_date}'
                         ''')
conn.commit()


# Inserting Data into COMMISSION_Update_log
table_name = "COMMISSION_Update_log" 
batch_size = 1000
idf = commission_log_df

bulk_insert(table_name, idf, batch_size)