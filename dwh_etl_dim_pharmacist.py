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
sql = "INSERT INTO dim_pharmacist(pharmacist_id, name, phone, dob, address, salary) VALUES (%s, %s, %s, %s, %s, %s)"

with open('data_reconcile/pharmacist.csv', mode='r') as csv_reader:
    pharmacists = csv.reader(csv_reader, delimiter=',')
    next(pharmacists)
    for pharmacist in tqdm(pharmacists):
        cur.execute(sql, pharmacist)
    conn.commit()

logging.debug('Finish insert data postgre')
