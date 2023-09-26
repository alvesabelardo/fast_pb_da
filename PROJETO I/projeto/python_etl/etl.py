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

#Informações de envio
tabela_envio = olist_ordem_vendas.merge(olist_cep,
                            left_on='customer_id',
                            right_on='customer_id',
                            how='left')
tabela_envio.rename(columns={'order_id': 'id_ordem', 'order_approved_at': 'data_aprovacao', 'order_estimated_delivery_date': 'estimativa_entrega',  'customer_id': 'fk_olist_cep', 'order_status': 'status_venda', 'customer_zip_code_prefix': 'cep', 'customer_city': 'cidade', 'customer_state': 'estado'}, inplace=True)
tabela_envio = tabela_envio[['id_ordem', 'data_aprovacao','estimativa_entrega', 'status_venda', 'cep', 'cidade', 'estado', 'fk_olist_cep' ]]
tabela_envio.data_aprovacao = pd.to_datetime(tabela_envio.data_aprovacao)
tabela_envio.estimativa_entrega = pd.to_datetime(tabela_envio.estimativa_entrega)
# print(tabela_envio.dtypes)

#Informações de produtos
tabela_produtos = olist_produtos.merge(olist_ordem_items,
                            left_on='product_id',
                            right_on='product_id',
                            how='left' )
tabela_produtos['dimensoes'] = tabela_produtos.apply(lambda row: '{:.0f}x{:.0f}x{:.0f}'.format(row['product_width_cm'], row['product_height_cm'], row['product_length_cm']), axis=1)
tabela_produtos = tabela_produtos.drop(['product_width_cm', 'product_height_cm', 'product_length_cm'], axis=1)
tabela_produtos = tabela_produtos[['product_id', 'product_category_name', 'dimensoes', 'shipping_limit_date', 'price', 'freight_value', 'seller_id', 'order_id']]
tabela_produtos.rename(columns={'product_id': 'id_produto', 'product_category_name': 'categoria', 'shipping_limit_date':'limite_data_envio', 'price':'valor_produto', 'freight_value':'valor_frete', 'seller_id':'id_venda', 'order_id':'fk_id_tabela_envio'}, inplace=True)
tabela_produtos.limite_data_envio = pd.to_datetime(tabela_produtos.limite_data_envio)
# print(tabela_produtos.dtypes)

#Informações de pagamentos
tabela_pagamentos = olist_pagamentos.copy()
tabela_pagamentos = tabela_pagamentos[['order_id', 'payment_type', 'payment_installments','payment_value']]
tabela_pagamentos.rename(columns={ 'payment_type': 'tipo_pagamento', 'payment_installments': 'qtde_parcelas', 'payment_value':'valor' }, inplace=True)
# print(tabela_pagamentos.dtypes)


#Inserindo dados nas tabelas
 
tabela_envio.to_sql(name='vendas', con=conn, if_exists='append', index=False, schema='ecommerce')
tabela_produtos.to_sql(name='produtos', con=conn, if_exists='append', index=False, schema='ecommerce')
tabela_pagamentos.to_sql(name='pagamentos', con=conn, if_exists='append', index=False, schema='ecommerce')

conn.commit()
