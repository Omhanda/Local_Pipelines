
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
rfs_master = pd.read_excel(rfs_sheet, sheet_name='Borrower Details') 

date_cols = [
    "RFS_Date",
    "Latest_Date_of_Repayment",
    "Correct_Repayment_Date_Format",
    "Correct_RFS_Date_Format",
    "NA"
]

numeric_cols = [
    "Sr._No.",
    "Year",
    "Group_Members",
    "Tenure_(in_months)",
    "DPD",
    "BHK_block",
    "Vaibhavi_ID_(For_Rangde-Loan_ID)"
]

float_cols = [
    "RFS_Amt",
    "Admin_charges",
    "Total_Repayable_Amount",
    "Total_Repaid_Amount",
    "Total_Outstanding_Amount",
    "Monthly_Scheduled_EMI",
    "EMI_Received_for_the_recent_month",
    "Total_Amount_Overdue"
]

string_cols = [
    "Grantor",
    "Borrower_Category",
    "Month",
    "State",
    "Territory",
    "District",
    "Actual_Block",
    "Vatika",
    "Responsible_field_Person",
    "Contact_Numbar",
    "Contact_Numbar.1",
    "Borrower_Name",
    "Value_Chain",
    "RFS_Agreement",
    "RFS_Grantor",
    "RFS_Status",
    "Repayment_Report",
    "Repayment"
]

# Dates
for col in date_cols:
    if col in rfs_master.columns:
        rfs_master[col] = pd.to_datetime(rfs_master[col], errors="coerce")

# Numbers
for col in numeric_cols:
    if col in rfs_master.columns:
        rfs_master[col] = pd.to_numeric(rfs_master[col], errors="coerce").fillna(0).astype('int')

# floats
for col in float_cols:
    if col in rfs_master.columns:
        rfs_master[col] = pd.to_numeric(rfs_master[col], errors="coerce").fillna(0.0).astype('float')

# Strings
for col in string_cols:
    if col in rfs_master.columns:
        rfs_master[col] = rfs_master[col].fillna('').astype("string")


rfs_master_2 = rfs_master.astype(object)
rfs_master_2 = rfs_master_2.where(pd.notnull(rfs_master), None)


rfs_master_2 = rfs_master_2[['Sr._No.', 'Grantor', 'Borrower_Category', 'Year', 'Month', 'State',
       'Territory', 'District', 'Actual_Block', 'BHK_block', 'Vatika',
       'Responsible_field_Person', 'Contact_Numbar', 'Borrower_Name',
       'Contact_Numbar.1', 'Group_Members', 'Vaibhavi_ID_(For_Rangde-Loan_ID)',
       'Value_Chain', 'RFS_Agreement', 'RFS_Date', 'Tenure_(in_months)',
       'RFS_Grantor', 'RFS_Amt', 'Admin_charges', 'Total_Repayable_Amount',
       'Total_Repaid_Amount', 'Total_Outstanding_Amount',
       'Monthly_Scheduled_EMI', 'RFS_Status',
       'EMI_Received_for_the_recent_month', 'Total_Amount_Overdue',
       'Repayment_Report', 'Latest_Date_of_Repayment', 'DPD',
       'Repayment', 'Correct_Repayment_Date_Format',
       'Correct_RFS_Date_Format']]

rfs_master_2.rename(columns={'Sr._No.': 'Sr_No' , 'Contact_Numbar.1':'Contact_Numbar_1', 'Vaibhavi_ID_(For_Rangde-Loan_ID)': 'Vaibhavi_ID_For_Rangde_Loan_ID', 'Tenure_(in_months)': 'Tenure_in_months'}, inplace=True)

rfs_master_2['inserted_on'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))


table_name = "rfs_master"  # Replace with your actual table name
batch_size = 1000
idf = rfs_master_2

bulk_insert(table_name, idf, batch_size, prod_cursor)

