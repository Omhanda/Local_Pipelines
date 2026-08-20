


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


prod_servr = os.environ.get("DB2_HOST")
prod_user = os.environ.get("DB2_USER")
prod_pass = os.environ.get("DB2_PASS")
prod_db = os.environ.get("DB2_NAME") 

conn = pyodbc.connect(
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={prod_servr};"  # Production server
            f"Database={prod_db};"  # Production database
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
    "https://www.googleapis.com/auth/drive"
]

# Authorize
creds = Credentials.from_service_account_file(service_acc_key, scopes=SCOPES)
gc = gspread.authorize(creds)


order_booking = 'https://docs.google.com/spreadsheets/d/19RvB03N1Az6ORh4-UPk4JPLMbWnTv6A5pfpwbgcPVBc/edit?usp=sharing'
order_booking_sheet = gc.open_by_url(order_booking)
order_booking_df = pd.DataFrame(order_booking_sheet.worksheet('Clean Order').get_all_records())


order_booking_df['Select_Delivery_Date'] = pd.to_datetime(
    order_booking_df['Select_Delivery_Date'].str.strip(),
    format='%d-%m-%Y'
)

order_booking_df['Changed_date'] = pd.to_datetime(
    order_booking_df['Changed_date'].str.strip(),
    format='%d-%m-%Y'
)

order_booking_df['lat_code'] = pd.to_numeric(
    order_booking_df['lat_code'].astype(str).str.strip(),
    errors='coerce'
)

order_booking_df['long_code'] = pd.to_numeric(
    order_booking_df['long_code'].astype(str).str.strip(),
    errors='coerce'
)

order_booking_df['Product_Price'] = pd.to_numeric(
    order_booking_df['Product_Price'].astype(str).str.strip(),
    errors='coerce'
)

order_booking_df['route_id_str'] = order_booking_df['route_id'].apply(lambda x: f"{x:05d}")

# Choose Order
rrp = 91110
route = '00688' 
date1 = '14-03-2026'
date1 = pd.to_datetime(date1, dayfirst=True)
user = 62840
officeid = 66

# Filtering
order_booking_df2 = order_booking_df[(order_booking_df['kiosk_code'] == rrp) & (order_booking_df['route_id_str'] == route) & (order_booking_df['Changed_date'] == date1) & (order_booking_df['userCode'] == user)].copy()
# order_booking_df2.values

order_booking_df2['Select_Delivery_Date'] = order_booking_df2['Changed_date']

print(order_booking_df2)
if order_booking_df2.empty:
    print("Empty Dataframe...!")
    pass

orderbooking_key = order_booking_df2.groupby([
    "kiosk_code",
    "route_id_str",
    "Select_Delivery_Date",
    "Office_Id",
    "lat_code",
    "long_code",
    "userCode"
])

print('\nOrderbooking\nkiosk_code ',
    'route_id_str ',
    'Select_Delivery_Date ',
    'Office_Id ',
    'lat_code ',
    'long_code ',
    'userCode\n')

for i in orderbooking_key.groups:
    print(i)


for key, order_df in orderbooking_key:

    kiosk_code, route_id_str, delivery_date, office_id, lat, long, userCode = key

    print(f"\nCreating Order for Kiosk kiosk_code, route_id, delivery_date, office_id, lat, long, userCode \n{kiosk_code, route_id_str, delivery_date, office_id, lat, long, userCode}")

    # STEP 1 : CREATE ORDER HEADER
    prod_cursor.execute("""
    EXEC drishteeDhavak..sp_scm__rrp_order_booking_insert
        ?,?,?,?,?,?
    """,
    int(kiosk_code),
    str(route_id_str),
    pd.to_datetime(delivery_date).to_pydatetime(),
    int(userCode),
    float(long),
    float(lat)
    )

    result = prod_cursor.fetchone()

    messagecode = result[0]
    message = result[1]
    order_id = result[2]
    
    print("Order Created:", order_id)
    print("Message Code:", messagecode, "Message:", message, "Order ID:", order_id)

    # STEP 2 : PROCESS PRODUCTS
    for _, row in order_df.iterrows():

        product_id = row['Product_Id']
        qty = pd.to_numeric(row['Type_Quantity'], errors='coerce')

        prod_cursor.execute("""
        EXEC drishtee_group_mis..usp_check_dhavak_stock
            ?,?,?
        """,
        int(product_id),
        int(office_id),
        pd.to_datetime(delivery_date).to_pydatetime()
        )

        stock = prod_cursor.fetchone()

        if stock is None:
            print(f"\nSkipping Product {product_id} (No stock record found)")
            prod_cursor.commit()
            conn.commit()
            continue
        
        product_id = stock[0]
        product_name = stock[1]
        available_quantity = stock[2]
        stock_available = stock[5]

        if float(stock_available) >= qty:

            print(f"\nInserting Product {product_id} for orderId {order_id}")

            prod_cursor.execute("""
            EXEC drishteeDhavak..sp_scm__rrp_order_booking_detail_insert
                ?,?,?,?
            """,
            int(order_id),
            int(userCode),
            int(product_id),
            int(qty)
            )

            prod_cursor.commit()
            conn.commit()
            
        else:
            print(f"Skipping Product {product_id} ,for route_id:{route_id_str}, kiosk_code:{kiosk_code}, lat:{lat}, long:{long}, delivery_date:{delivery_date}, office_id:{office_id}, userCode:{userCode},  OrderID:{order_id}")
    
    prod_cursor.commit()
    conn.commit()

print("Orderbooking Data inserted successfully...!")


order_booking_df2['rrp_u'] = (
    'D~' + order_booking_df2['route_id_str']
    .astype(str)
    .str.replace(r'[\n\r]', '', regex=True)
    .str.strip()
)

clean_order_booking_df = order_booking_df2.copy()


import_to_dhavak_key = clean_order_booking_df.groupby([
    'rrp_u',
    'route_id_str',
    'Select_Delivery_Date',
    'userCode',
    'Office_Id'
])

print('\nImport to Dhavak\nrrp_u ',
    'route_id_str ',
    'Select_Delivery_Date ',
    'userCode ',
    'Office_Id ')

for i in import_to_dhavak_key.groups:
    print(i)

for keys, order_df in import_to_dhavak_key:
    rrp_u, route_id_str, delivery_date, userCode, office_id = keys

    print(f"\nProcessing Order → RRP:{rrp_u} route:{route_id_str} delivery_date:{delivery_date} userCode:{userCode} office_id:{office_id}")

    delivery_date = pd.to_datetime(delivery_date).to_pydatetime()

    prod_cursor.execute("""
    EXEC drishtee_group_mis..OrderBooking_Insert_proc ?,?,?,?,?
    """,
    str(rrp_u),
    str(route_id_str),
    delivery_date,
    int(userCode),
    int(office_id)
    )

    result = prod_cursor.fetchone()
    order_id = result[0]
    print("Order ID:", order_id)

    if result is None:
        print("Order creation failed\n")
        continue

    print(f"Fetching stock for route_id:{route_id_str}, delivery_date:{delivery_date}, office_id:{office_id}")

    prod_cursor.fast_executemany = True

    prod_cursor.execute("""
    EXEC drishtee_group_mis..select_orderfromdhavak_with_stock_proc ?,?,?
    """,
    str(route_id_str),
    delivery_date,
    int(office_id)
    )
    columns = [col[0] for col in prod_cursor.description]
    stock_rows = [tuple(i) for i in prod_cursor.fetchall()]
    stock_df = pd.DataFrame.from_records(stock_rows, columns=columns)

    stock_df1 = stock_df[~stock_df['unit_price'].isna()].copy()

    if not stock_df1.shape[0]:
        print("No stock returned\n")
        continue
    
    merged_df = pd.merge(
    clean_order_booking_df,
    stock_df1,
    left_on=['Product_Id', 'route_id_str', 'Select_Delivery_Date', 'Office_Id'],
    right_on=['product_id', 'route_id', 'delivery_date', 'office_id'],
    how='inner'
    )

    merged_df1 = merged_df[['product_id', 'quantity', 'route_id_y', 'office_id', 'delivery_date',
        'tax', 'unit_price', 'product_name', 'peti_quantity', 'tax_amount',
        'available_quanitty', 'last_purchase', 'brand_name']].drop_duplicates().copy()

    for row in merged_df1.itertuples():

        print(f"Inserting Product {row.product_id} for orderId {order_id}")

        prod_cursor.execute("""
        EXEC drishtee_group_mis..OrderBookingDetail_Insert_proc
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?
        """,
        int(order_id),
        int(row.product_id),
        float(row.quantity or 0),
        float(row.unit_price or 0),
        int(userCode),
        float(row.tax or 0),
        float(row.tax_amount or 0),
        int(office_id),
        float(row.peti_quantity or 0),
        float(row.unit_price or 0),
        float(row.unit_price or 0),
        float(0),
        'F',
        'N'
        )

    prod_cursor.commit()
    conn.commit()

    print(f"Order Completed for: RRP={rrp_u}, route_id={route_id_str}, delivery_date={delivery_date}, office_id={office_id}, userCode={userCode}, order_id={order_id}")

print(f"\Imported To Dhavak...!")



issue_to_executive_key = clean_order_booking_df.groupby(['userCode', 'Select_Delivery_Date','route_id_str', 'Office_Id'])

print('\nIssue to executive\nuserCode ', 'Select_Delivery_Date ','route_id_str ', 'Office_Id')
for i in issue_to_executive_key.groups:
    print(i)


for keys, order_df in issue_to_executive_key:
    userCode, Delivery_Date,route_id_str, Office_Id = keys

    print(f"\nIssuing Order :\nuserCode:{userCode} Delivery_Date:{Delivery_Date} route_id:{route_id_str} Office_Id:{Office_Id}")

    delivery_date = pd.to_datetime(Delivery_Date).to_pydatetime()

    prod_cursor.execute("""
    SELECT CONCAT(LTRIM(TRIM(UserFirstName)),' ',LTRIM(TRIM(UserMiddleName)),' ',LTRIM(TRIM(UserLastName))) transporter_name
    FROM drishteeDhavak..usermaster WHERE UserDrishteeID = ?
    """,
    int(userCode)
    )
    columns = [col[0] for col in prod_cursor.description]
    rows = [tuple(i) for i in prod_cursor.fetchall()]
    dhavak = pd.DataFrame.from_records(rows, columns=columns)

    print(f"Issue For Dhavak: {dhavak.transporter_name[0]}\n")
    
    # prod_cursor.execute("""
    # drishtee_group_mis..ProductIssueRRA_insert_update ?,?,?,?,?,?,?,?,?,?
    # """,
    # int(userCode),                                      
    # delivery_date,                                      
    # 'EMP967',                                      
    # str(route_id_str),                                    
    # 'I',                                      
    # int(Office_Id),                  
    # str(dhavak.transporter_name[0]),                   
    # '1234',                    
    # 10,                    
    # 1
    # )
    prod_cursor.execute("""
    drishtee_group_mis..ProductIssueRRA_insert_update_1 ?,?,?,?,?,?,?,?,?,?
    """,
    int(userCode),                                      
    delivery_date,                                      
    'EMP967',                                      
    str(route_id_str),                                    
    'I',                                      
    int(Office_Id),                  
    str(dhavak.transporter_name[0]),                   
    '1234',                    
    10,                    
    1
    )

prod_cursor.commit()
conn.commit()

print("\nRRA Issued Completed..!")


sale_entry_key = clean_order_booking_df.groupby(['kiosk_code','route_id_str', 'Select_Delivery_Date','userCode'])

print("\nSale Entry :")

for i in sale_entry_key.groups:
    print(i)

for key , order_df in sale_entry_key:

    kiosk_code, route_id_str, delivery_date, userCode = key

    delivery_date = pd.to_datetime(delivery_date).to_pydatetime()

    print(f"\nInserting Order for Kiosk {kiosk_code}, Route {route_id_str}, Delivery Date {delivery_date}, User {userCode}")

    prod_cursor.execute("""
    EXEC drishteeDhavak..sp_scm_rrp_sale_booking_insert ?,?,?,?
                        """,
                        int(kiosk_code),
                        str(route_id_str),
                        delivery_date,
                        int(userCode)
                    )
    
    print("Inserted...!")
    
prod_cursor.commit()
conn.commit()

print("Sale Entry Completed..!")
    