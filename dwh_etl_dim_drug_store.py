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
sql = "INSERT INTO dim_drug_store(drug_store_id, name, location) VALUES (%s, %s, %s)"

with open('data_reconcile/drug_store.csv', mode='r') as csv_reader:
    drug_stores = csv.reader(csv_reader, delimiter=',')
    next(drug_stores)
    for customer in tqdm(customers):
        cur.execute(sql, customer)
    conn.commit()

logging.debug('Finish insert data postgre')
