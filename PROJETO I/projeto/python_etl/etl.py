import pandas as pd
from sqlalchemy import create_engine

#Conexão
conn = create_engine('postgresql://postgres:estagiocompass@projeto-postgres-1/postgres').connect()

#Importando CSV's
olist_produtos = pd.read_csv('/docker-entrypoint-initdb.d/olist_products_dataset.csv')
olist_ordem_items = pd.read_csv('/docker-entrypoint-initdb.d/olist_order_items_dataset.csv')
olist_pagamentos = pd.read_csv('/docker-entrypoint-initdb.d/olist_order_payments_dataset.csv')
olist_ordem_vendas = pd.read_csv('/docker-entrypoint-initdb.d/olist_orders_dataset.csv')
olist_cep = pd.read_csv('/docker-entrypoint-initdb.d/olist_customers_dataset.csv')

## order_reviews = pd.DataFrame('./mongodb-init/init.js')

tabela_produtos = olist_ordem_vendas.merge(olist_cep,
                            left_on='customer_id',
                            right_on='customer_id',
                            how='left')

tabela_produtos.rename(columns={'order_id': 'id', 'order_approved_at': 'data_aprovacao', 'order_estimated_delivery_date': 'estimativa_entrega',  'customer_id': 'fk_olist_cep', 'order_status': 'status_venda', 'customer_zip_code_prefix': 'cep', 'customer_city': 'cidade', 'customer_state': 'estado'}, inplace=True)
tabela_produtos = tabela_produtos[['id', 'data_aprovacao','estimativa_entrega','fk_olist_cep', 'status_venda', 'cep', 'cidade', 'estado' ]]
tabela_produtos.data_aprovacao = pd.to_datetime(tabela_produtos.data_aprovacao)
tabela_produtos.estimativa_entrega = pd.to_datetime(tabela_produtos.estimativa_entrega)
# print(tabela_produtos.dtypes)

#Inserindo tabela produtos 
tabela_produtos.to_sql(name='tabela_produtos', con=conn, if_exists='append', index=False, schema='ecommerce')

conn.commit()
