
import pyodbc
import os
import pandas as pd
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe
from dotenv import load_dotenv , find_dotenv # to load .env files

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
service_acc_key = os.environ.get("gcp_bot_key")

# Define the required scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authorize
creds = Credentials.from_service_account_file(service_acc_key, scopes=SCOPES)
gc = gspread.authorize(creds)

csp_list_url = 'https://docs.google.com/spreadsheets/d/1rVg5FKcyeg1Df4oUdc6ak-wka_YO4e8Gxs_FBexK5LU/edit?gid=0#gid=0'

csp_sheet = gc.open_by_url(csp_list_url)

csp_df = pd.DataFrame(csp_sheet.worksheet('Sheet1').get_all_records())

warehouse_cursor.execute("SELECT * FROM WAVE..CSP_Master")

rows_tuple = [tuple(i) for i in warehouse_cursor.fetchall()]

column = [column[0] for column in warehouse_cursor.description]

csp_master_df = pd.DataFrame(rows_tuple, columns=column)

print(f"CSP_Master Records --> {len(csp_master_df)} \nNew CSP Records --> {len(csp_df)} \n")

# TRANSFORMATIONS
csp_df['PINCODE'] = pd.to_numeric(csp_df['PINCODE'], errors='coerce').fillna(0).astype('int')
csp_df['MATM'] = pd.to_numeric(csp_df['MATM'], errors='coerce').fillna(0).astype('int')
csp_df['PinPad'] = pd.to_numeric(csp_df['PinPad'], errors='coerce').fillna(0).astype('int')
csp_df['Vatika ID'] = pd.to_numeric(csp_df['Vatika ID'], errors='coerce').fillna(0).astype('int')
csp_df['MR No.'] = pd.to_numeric(csp_df['MR No.'], errors='coerce').fillna(0).astype('int')
csp_df['Licence Fee\n Amount'] = pd.to_numeric(csp_df['Licence Fee\n Amount'], errors='coerce').fillna(0.0)
csp_df['Received Amount'] = pd.to_numeric(csp_df['Received Amount'], errors='coerce').fillna(0.0)

fill_values = {
    'BANK': "",
    'CSPCODE': "",
    'CSP Name': "",
    'Key': "",
    'State': "",
    'Territory': "",
    'District': "",
    'BLOCK': "",
    'Status': "",
    'Branch': "",
    'Code Creation Date': '1999-01-01',        # keep datetime Null
    'Agreement Date': '1999-01-01',
    'Agreement Renewal Date': '1999-01-01',
    'IIBF Certificate\nNumber': "",
    'Printer': "",
    'MR Date': '1999-01-01',
    'Vatika Name': ""
}

for col, default_value in fill_values.items():
    if col in csp_df.columns:
        csp_df[col] = csp_df[col].fillna(default_value)

dtype_map = {
    'BANK': 'object',
    'CSPCODE': 'object',
    'CSP Name': 'object',
    'Key': 'object',
    'State': 'object',
    'Territory': 'object',
    'District': 'object',
    'BLOCK': 'object',
    'Status': 'object',
    'Branch': 'object',
    'PINCODE': 'int',
    'Code Creation Date': 'datetime64[ns]',
    'Agreement Date': 'datetime64[ns]',
    'Agreement Renewal Date': 'datetime64[ns]',
    'IIBF Certificate\nNumber': 'object',
    'Printer': 'object',
    'MATM': 'int',
    'PinPad': 'int',
    'Licence Fee\n Amount': 'float',
    'MR Date': 'datetime64[ns]',
    'MR No': 'int',
    'Received Amount': 'float',
    'Vatika ID': 'int',
    'Vatika Name': 'object'
}

# Apply safe conversions
for col, new_type in dtype_map.items():
    if col in csp_df.columns:
        try:
            if "datetime" in new_type:
                csp_df[col] = pd.to_datetime(csp_df[col], errors='coerce')
            else:
                csp_df[col] = csp_df[col].astype(new_type)
        except Exception as e:
            print(f"Datatype conversion failed for {col}: {e}")

csp_df['Code Creation Date'] = pd.to_datetime(csp_df['Code Creation Date'])
csp_df['Agreement Date'] = pd.to_datetime(csp_df['Agreement Date'])
csp_df['Agreement Renewal Date'] = pd.to_datetime(csp_df['Agreement Renewal Date'])
# csp_df['Licence Fee Refund Date'] = pd.to_datetime(csp_df['Licence Fee Refund Date'])
csp_df['MR Date'] = pd.to_datetime(csp_df['MR Date'])
# csp_df['Licence Fee Amount'] = csp_df['Licence Fee Amount'].astype('float')
csp_df['Received Amount'] = csp_df['Received Amount'].astype('float') 
csp_df['Vatika ID'] = csp_df['Vatika ID'].astype('int')

# Trimming Keys
csp_df['CSPCODE'] = csp_df['CSPCODE'].astype(str).str.strip()
csp_master_df['CSPCODE'] = csp_master_df['CSPCODE'].astype(str).str.strip()

# Remove 'Cancelled' and 'TBA' entries from csp_master_df
csp_master_df_1 = csp_master_df[~csp_master_df['CSPCODE'].isin(['Cancelled', 'TBA'])]

csp_master_df_1 = csp_master_df_1.drop_duplicates()

# Joining CSP DataFrames
csp_1 = pd.merge(csp_df, csp_master_df_1, left_on='CSPCODE', right_on = 'CSPCODE', how='left')

# Selecting and Renaming Columns
csp_1 = csp_1[['BANK_x', 'CSPCODE', 'CSP Name', 'State_x', 'Territory_x', 'District_x',
       'BLOCK_x', 'bhk_block_code', 'Status_x', 'Branch_x', 'PINCODE_x', 'Code Creation Date',
       'Agreement Date', 'Agreement Renewal Date', 'IIBF Certificate\nNumber',
       'Printer_x', 'MATM_x', 'PinPad_x', 'Licence Fee\n Amount', 'MR Date',
       'MR No.', 'Received Amount', 'Vatika ID', 'Vatika Name','Key']]

csp_1 = csp_1.rename(columns={
    'BANK_x' : 'BANK',
    'CSP Name': 'CSP_Name',
    'State_x': 'State',
    'Territory_x': 'Territory',
    'District_x': 'District',
    'BLOCK_x': 'BLOCK',
    'Branch_x': 'Branch',
    'PINCODE_x': 'PINCODE',
    'Code Creation Date': 'Code_Creation_Date',
    'Agreement Date': 'Agreement_Date',
    'Agreement Renewal Date': 'Agreement_Renewal_Date',
    'IIBF Certificate\nNumber': 'IIBF_Certificate_Number',
    'Printer_x': 'Printer',
    'MATM_x': 'MATM',
    'PinPad_x': 'PinPad',
    'Licence Fee\n Amount': 'Licence_Fee_Amount',
    'MR Date': 'MR_Date',
    'MR No.': 'MR_No',
    'Received Amount': 'Received_Amount',
    'Vatika ID': 'Vatika_ID',
    'Vatika Name': 'Vatika_Name',
    'Status_x': 'Status'
    # Any extra columns (Refund Amount, Employee Mapped etc.) will remain
})

# Replacing NULL Values
csp_1['bhk_block_code'] = csp_1['bhk_block_code'].fillna(0)
csp_1['bhk_block_code'] = csp_1['bhk_block_code'].astype(int)

# csp_1['Code_Creation_Date'].fillna('2000-01-01', inplace=True)
# csp_1['Agreement_Date'].fillna('2000-01-01', inplace=True)
# csp_1['Agreement_Renewal_Date'].fillna('2000-01-01', inplace=True)
# csp_1['MR_Date'].fillna('2000-01-01', inplace=True)

date_cols = [
    'Code_Creation_Date',
    'Agreement_Date',
    'Agreement_Renewal_Date',
    'MR_Date'
]
csp_1[date_cols] = csp_1[date_cols].fillna('2000-01-01')

csp_1['Key'] = csp_1['Key'].fillna('')

csp_1 = csp_1.drop_duplicates()

backup = warehouse_cursor.execute('''
                        USE WAVE
                                  
                        TRUNCATE TABLE CSP_Master
                                  
                        INSERT INTO CSP_Master_tmp
                            SELECT
                            [BANK]
                            ,[CSPCODE]
                            ,[CSP_Name]
                            ,[Key]
                            ,[State]
                            ,[Territory]
                            ,[District]
                            ,[BLOCK]
                            ,[bhk_block_code]
                            ,[Status]
                            ,[Branch]
                            ,[PINCODE]
                            ,[Code_Creation_Date]
                            ,[Agreement_Date]
                            ,[Agreement_Renewal_Date]
                            ,[IIBF_Certificate_Number]
                            ,[Printer]
                            ,[MATM]
                            ,[PinPad]
                            ,[Licence_Fee_Amount]
                            ,[MR_Date]
                            ,[MR_No]
                            ,[Received_Amount]
                            ,[Vatika_ID]
                            ,[Vatika_Name]
                            FROM [CSP_Master]
                                  
                            TRUNCATE TABLE [CSP_Master]
                        ''')

conn.commit()

if backup:
    print("CSP_Master Backup Created Successfully...!\n")

warehouse_cursor.execute("SELECT * FROM WAVE..CSP_Master")
rows_tuple = [tuple(i) for i in warehouse_cursor.fetchall()]
column = [column[0] for column in warehouse_cursor.description]
csp_master_df_check = pd.DataFrame(rows_tuple, columns=column)
conn.commit()

if len(csp_master_df_check) == 0:
    print(f"CSP_Master Truncated...! {len(csp_master_df_check)} Records Found")
else:
    print(f"{len(csp_master_df_check)} Records Found")

# INSERTING DATA INTO WAREHOUSE
table_name = "CSP_Master"  # Replace with your actual table name
batch_size = 1000
idf = csp_1

try:
    columns = idf.columns.tolist()
    # columns_str = ', '.join(columns)
    columns_str = ', '.join([f'[{col}]' for col in columns])
    placeholders = ', '.join(['?' for _ in range(len(columns))])
    insert_query = f"INSERT INTO WAVE..{table_name} ({columns_str}) VALUES ({placeholders})"
    
    total_inserted = 0
    
    # Process in batches
    for i in range(0, len(idf), batch_size):
        batch = idf.iloc[i: i + batch_size]
        records = [tuple(row) for _, row in batch.iterrows()]
        
        print(f"Executing batch insert for records {i} to {i + len(records)}")
        warehouse_cursor.executemany(insert_query, records)
        
        total_inserted += len(records)
        print(f"Inserted batch: {total_inserted}/{len(idf)} records")
              
    
    print(f"✓ Successfully inserted all {total_inserted} records into {table_name}")
    conn.commit()
    conn.close()
    
except Exception as e:
    # conn.rollback()
    print(f"✗ Error inserting data: {str(e)}")