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
        INSERT INTO purchase (sr_no, reference_no, journal_date, purchase_date, expiry_date, 
                              purchaser_name, prefix, bond_number, denominations, issue_branch_code, 
                              issue_teller, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)
    
    for row in data:
        cur.execute(insert_query, row)
    conn.commit()
    cur.close()
    conn.close()    

db_config = {
    'dbname': 'postgres', 
    'user': 'postgres',
    'password': 'Milkyway@1',
    'host': '18.191.145.248',
    'port': '5432'
}

excel_path = 'purchase.xlsx'
raw_data = extract_data_from_excel(excel_path)
insert_data_to_db(raw_data, db_config)
