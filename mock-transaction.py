from tqdm import tqdm
import logging

import psycopg2
import csv

# config
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s'
)

host = 'localhost'
database = 'dwh'
user = 'postgres'
password = 'postgress'

# connection
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
cur = conn.cursor()

logging.debug('Start insert data postgre')

# single insert in iteration
def get_pharmacist_data():
    cur.execute("SELECT * FROM dim_customer")
    return cur.fetchall()

def get_customers_data():
    cur.execute("SELECT * FROM dim_pharmacist")
    return cur.fetchall()

def get_drug_store_data():
    cur.execute("SELECT * FROM dim_drug_store")
    return cur.fetchall()

def get_drug_data():
    cur.execute("SELECT * FROM dim_drug")
    return cur.fetchall()

def get_date_data():
    cur.execute("SELECT * FROM dim_date")
    return cur.fetchall()

