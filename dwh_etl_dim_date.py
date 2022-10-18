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

logging.debug('Start Reading csv file')

# single insert in iteration
sql = "INSERT INTO dim_date(date, month, year) VALUES (%s, %s, %s)"

dates = []

with open('data_reconcile/transaction_data.csv', mode='r') as csv_reader:
    transaction_data = csv.reader(csv_reader, delimiter=',')
    next(transaction_data)
    for data in tqdm(transaction_data):
        dates.append(data[3])

logging.debug('Finish Reading csv file')

distinct_dates = list(dict.fromkeys(dates))

sql = "INSERT INTO dim_date(date, month, year) VALUES (%s, %s, %s)"

logging.debug('Start insert data postgre')

for date in tqdm(distinct_dates):
    value = (date, int(date[5:7]), int(date[0:4]))
    cur.execute(sql, value)
conn.commit()


logging.debug('Finish insert data postgre')
