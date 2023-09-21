import pandas as pd
# import os
import psycopg2
from psycopg2 import extensions
# from pymongo import MongoClient
# connection_string = "mongodb://mongoadmin:estagiocompass@localhost:27017/?authSource=admin"
# client = MongoClient(connection_string)
# db_connection = client["ecommerce"]
# print(db_connection)

# commit = extensions.ISOLATION_LEVEL_AUTOCOMMIT
# conn = psycopg2.connect(
#     host="localhost",
#     database="input",
#     user="admin",
#     password="estagiocompass"
# )
# conn.set_isolation_level(commit)
# cursor = conn.cursor()
# cursor.execute("SELECT * from produtos")

# result = cursor.fetchall()
# print()

produtos = pd.read_csv('./input/olist_products_dataset.csv')
ordem_items = pd.read_csv('./input/olist_order_items_dataset.csv')
pagamentos = pd.read_csv('./input/olist_order_payments_dataset.csv')
ordem_vendas = pd.read_csv('./input/olist_orders_dataset.csv')
cep = pd.read_csv('./input/olist_customers_dataset.csv')

venda_info = ordem_vendas.merge(cep,
                            left_on='customer_id',
                            right_on='customer_id',
                            how='left')

venda_info = venda_info[['order_status','order_estimated_delivery_date','customer_city', 'customer_state']]

print(venda_info) 