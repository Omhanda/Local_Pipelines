
import pyodbc
import os
import io
import pandas as pd
from datetime import datetime, date

import gspread
from gspread_dataframe import get_as_dataframe

from google.oauth2 import service_account
from google.oauth2.service_account import Credentials

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from dotenv import load_dotenv , find_dotenv # to load .env files
# from google.cloud import storage

# client = storage.Client()
load_dotenv() # Load .env file


prod_servr = os.environ.get("DB2_HOST")
prod_user = os.environ.get("DB2_USER")
prod_pass = os.environ.get("DB2_PASS")
prod_db = os.environ.get("DB2_NAME") 

conn = pyodbc.connect(
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={prod_servr};"  # Warehouse server
            f"Database={prod_db};"  # Warehouse database
            f"UID={prod_user};"
            f"PWD={prod_pass};"
        )

prod_cursor = conn.cursor()

prod_cursor.fast_executemany = True


# Path to your downloaded key
service_acc_key = os.getenv("gcp_bot_key")

# Define the required scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    # "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.readonly"
]

# Authorize
creds = Credentials.from_service_account_file(service_acc_key, scopes=SCOPES)
gc = gspread.authorize(creds)


def bulk_insert(table_name, data, batch_size , cursor):
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
            cursor.executemany(insert_query, records)
            
            total_inserted += len(records) 
            print(f"Inserted batch: {total_inserted}/{len(data)} records")
                
        
        print(f"✓ Successfully inserted all {total_inserted} records into {table_name}")
        cursor.connection.commit()
        # conn.close()
        
    except Exception as e:
        # conn.rollback()
        print(f"✗ Error inserting data: {str(e)}")
        
    # finally:
    #     warehouse_cursor.close()
    #     conn.close()


drive = build( "drive", "v3", credentials=creds)

# Id
rfs_file_id = "1FilWyVH4o0k_VnGOgfTVIGTgRX1iQgDY"

# Verifying Drive can see the file.
file = drive.files().get(fileId=rfs_file_id,fields="id,name,mimeType").execute()


# Download the Excel File into Memory 
# Instead of saving the file to disk, we'll download it into RAM (BytesIO).

# Create a request to download the file
request = drive.files().get_media(fileId=rfs_file_id)

# Create an in-memory binary stream
excel_stream = io.BytesIO()

# Download the file
downloader = MediaIoBaseDownload(excel_stream, request)

done = False

while not done:
    status, done = downloader.next_chunk()
    print(f"Downloaded: {int(status.progress() * 100)}%")

# Move pointer back to the beginning
excel_stream.seek(0)

print("Download Completed Successfully!")

rfs_sheet = pd.ExcelFile(excel_stream)
rfs_producers = pd.read_excel(rfs_sheet, sheet_name='Producer details') 

rfs_producers.rename(columns={'livelihood_earning_current_(monthly)':'livelihood_earning_current_monthly','Income_Change_(₹)': 'Income_Change', 'Working_Since_(in months)':'Working_Since_in_months','worked_on__how_many_value_chains__before_this':'worked_on_how_many_value_chains_before_this',
                              'personal_business/group_business/labour_basis_with_other_groups':
                              'persol_business_group_business_labour_basis_with_other_groups',
                              '_livelihood_options_according_to_them_with_Rs_200_every_day_for_the_entire_year__':'livelihood_options_according_to_them_with_Rs_200_every_day_for_the_entire_year'}, inplace=True)

numeric_cols = ['Sr_NO','VaibhaviId','worked_on_how_many_value_chains_before_this']

float_cols = ["Income_Change", 'livelihood_earning_current_monthly','Working_Since_in_months','%_Change',
              'livelihood_earning_from_each_value_chain_before_this']

string_cols = ['State', 'District', 'Name_of_the_producer', 'Contact_No',
       'Associated_with_vaibhavi',
       'vaibhavi_value_chain', 'helped_the_group_in',
       'name_of_the_value_chains_worked_on_earlier',
       'persol_business_group_business_labour_basis_with_other_groups',
       'Income_Status', 'Experience_Level', 'Diversification_Status',
       'livelihood_options_according_to_them_with_Rs_200_every_day_for_the_entire_year'
      ]

# Numbers
for col in numeric_cols:
    if col in rfs_producers.columns:
        rfs_producers[col] = pd.to_numeric(rfs_producers[col], errors="coerce").fillna(0).astype('int')

# floats
for col in float_cols:
    if col in rfs_producers.columns:
        rfs_producers[col] = pd.to_numeric(rfs_producers[col], errors="coerce").fillna(0.0).astype('float')

for col in string_cols:
    if col in rfs_producers.columns:
        rfs_producers[col] = rfs_producers[col].fillna('').astype("string")

rfs_producers['inserted_on'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))

table_name = "RFS_Producers"  # Replace with your actual table name
batch_size = 1000
idf = rfs_producers

bulk_insert(table_name, idf, batch_size, prod_cursor)