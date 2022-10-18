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

logging.debug('Start reading data transaction')

transaction_data_list = []

with open('data_reconcile/transaction_data.csv', 'r') as csv_reader:
    transaction_data = csv.reader(csv_reader, delimiter=',')
    next(transaction_data)
    for transaction in tqdm(transaction_data):
        transaction_data_list.append(transaction)

logging.debug('Finish reading data transaction')

logging.debug('Start insert data postgre')

for row in tqdm(transaction_data_list):
    customer_id = row[6]
    drug_id = row[2]

    # get date id
    cur_date_id = conn.cursor()
    cur_date_id.execute("SELECT date_id FROM dim_date WHERE date=%s", [row[3]])
    date_id = cur_date_id.fetchone()[0]

    drug_store_id = row[7]
    pharmacist_id = row[1]
    total_sales = row[4]

    # revenue = total * price_sell
    # revenue != total, jadi banyak minus
    cur_revenue = conn.cursor()
    cur_revenue.execute("SELECT %s*price_sell FROM dim_drug WHERE drug_id=%s", [total_sales, drug_id])
    revenue = float(cur_revenue.fetchone()[0])

    # expense = total * price_buy
    cur_expense = conn.cursor()
    cur_expense.execute("SELECT %s*price_buy FROM dim_drug WHERE drug_id=%s", [total_sales, drug_id])
    expense = float(cur_expense.fetchone()[0])

    income = revenue - expense

    sql = "INSERT INTO fact_recap_pharmacy(customer_id, drug_id, date_id, drug_store_id, pharmacist_id, revenue, expense, income, total_sales) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    value = (customer_id, drug_id, date_id, drug_store_id, pharmacist_id, revenue, expense, income, total_sales)
    cur.execute(sql, value)
    conn.commit()

logging.debug('Finish insert data postgre')
