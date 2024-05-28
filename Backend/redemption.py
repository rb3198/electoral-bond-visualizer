import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

def extract_data_from_excel(excel_path, start_row=1):
    df = pd.read_excel(excel_path, header=None)
    data = df.iloc[start_row:].values.tolist()    
    return data

def insert_data_to_db(data, db_config):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    insert_query = sql.SQL("""
        INSERT INTO redemption (sr_no, encashment_date, party_name, party_account_no, prefix, 
                                bond_number, denominations, pay_branch_code, pay_teller)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    for row in data:
        cur.execute(insert_query, row)
    conn.commit()
    cur.close()
    conn.close()

db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}
excel_path = 'redemption.xlsx'
raw_data = extract_data_from_excel(excel_path)
insert_data_to_db(raw_data, db_config)
