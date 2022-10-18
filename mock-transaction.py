from tqdm import tqdm
import logging

import psycopg2
import csv
import random
from faker import Faker

# Faker object
fake = Faker()

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

# single insert in iteration
def get_pharmacist_data():
    cur.execute("SELECT * FROM dim_pharmacist")
    return cur.fetchall()

def get_customers_data():
    cur.execute("SELECT * FROM dim_customer")
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

def get_random_data(data):
    return random.choice(data)

# store dimensional data
pharmacists = get_pharmacist_data()
drugs = get_drug_data()
drug_stores = get_drug_store_data()
customers = get_customers_data()

# transaction id format
id_format = "TR_SP{:05d}"

with open('data_reconcile/transaction_data.csv', 'w', encoding='UTF-8') as file:
    writer = csv.writer(file, lineterminator='\n')
    writer.writerow(['transaction_id', 'pharmacist_id', 'drug_id', 'date', 'qty_buy', 'total', 'customer_id', 'drug_store_id'])
    for i in range(10000):
        transaction_id = id_format.format(i)
        pharmacist_id = get_random_data(pharmacists)[0]
        drug = get_random_data(drugs)
        drug_id = drug[0]
        sell_price = drug[4]
        qty_buy = random.randint(1, 10)
        total = sell_price * qty_buy
        customer_id = get_random_data(customers)[0]
        drug_store_id = get_random_data(drug_stores)[0]
        date = fake.date_between(start_date='-2y', end_date='-1y').strftime("%Y-%m-%d")

        record = [transaction_id, pharmacist_id, drug_id, date, qty_buy, total, customer_id, drug_store_id]
        writer.writerow(record)
