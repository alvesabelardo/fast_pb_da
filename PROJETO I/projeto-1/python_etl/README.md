# Informações sobre o ETL

**Realizando as conexões com mongo e postgres**

```py
#Conexão Postgre
conn = create_engine('postgresql://postgres:estagiocompass@projeto-1-postgres-1/pb_wd').connect()
#Conexão Mongo 
client = MongoClient('mongodb://mongoadmin:estagiocompass@projeto-1-mongo-1:27017/?authSource=admin')
```

**Realizei a importação de dados do Mongo apagando o id criado automaticamente**

```py
#Importando e alterando dados Mongo
db = client.ecommerce.order_reviews
_ = db.find({}, {'_id':0})
order_reviews = pd.DataFrame(_)
```

**Importando os dados dos arquivos CSV diretamente do entrypoint do docker**


```py
olist_produtos = pd.read_csv('/docker-entrypoint-initdb.d/olist_products_dataset.csv')
olist_ordem_items = pd.read_csv('/docker-entrypoint-initdb.d/olist_order_items_dataset.csv')
olist_pagamentos = pd.read_csv('/docker-entrypoint-initdb.d/olist_order_payments_dataset.csv')
olist_ordem_vendas = pd.read_csv('/docker-entrypoint-initdb.d/olist_orders_dataset.csv')
olist_clientes = pd.read_csv('/docker-entrypoint-initdb.d/olist_customers_dataset.csv')
```

**Aqui comecei a fazer as alterações necessárias seguindo a modelagem de dados**

![Modelo Fato e Dimensão](/img/modelo-fato-dimensao.jpg)

Optei por fazer a tabela fato com os dados de vendas, e suas dimensões com colunas que são responsavéis por gerar as vendas.

Comecei alterando a tabela de produtos, alterando seus tipos de dados e renomeando suas colunas

```py
#Alterações tabela dim_produto_review
order_reviews.review_creation_date = pd.to_datetime(order_reviews.review_creation_date)
order_reviews.review_answer_timestamp = pd.to_datetime(order_reviews.review_answer_timestamp)
order_reviews.review_score = pd.to_numeric(order_reviews.review_score)
order_reviews.rename(columns={  'review_id':'prod_review_id',
                                'review_comment_title':'titule_comentario',
                                'review_comment_message':'comentario',
                                'review_creation_date':'data_criacao',
                                'review_answer_timestamp':'data_hora_pergunta',}, inplace=True)
# print(order_reviews.dtypes)
```

**Realizei os mesmos procedimentos para a tabela Fato**

```py
#Alterações da tabela fato_vendas
fato_vendas = olist_ordem_vendas
fato_vendas.order_purchase_timestamp = pd.to_datetime(fato_vendas.order_purchase_timestamp)
fato_vendas.order_approved_at = pd.to_datetime(fato_vendas.order_approved_at)
fato_vendas.order_delivered_carrier_date = pd.to_datetime(fato_vendas.order_delivered_carrier_date)
fato_vendas.order_delivered_customer_date = pd.to_datetime(fato_vendas.order_delivered_customer_date)
fato_vendas.order_estimated_delivery_date = pd.to_datetime(fato_vendas.order_estimated_delivery_date)
fato_vendas.rename(columns={'order_status':'status_venda',
                            'order_purchase_timestamp':'data_hora_venda',
                            'order_approved_at':'data_hora_aprovacao',
                            'order_delivered_carrier_date':'data_entregue_transport',
                            'order_delivered_customer_date':'data_entregue_cliente',
                            'order_estimated_delivery_date':'data_entrega_estimada',}, inplace=True)
# print(fato_vendas.dtypes)
```

**Realizei os mesmos procedimentos para a tabela de clientes**

```py
#Alterações da tabela dim_clientes
dim_clientes = olist_clientes
dim_clientes.customer_zip_code_prefix = pd.to_numeric(dim_clientes.customer_zip_code_prefix)
dim_clientes.rename(columns={'customer_unique_id':'id_cliente',
                          'customer_zip_code_prefix':'zip_code_client',
                          'customer_city':'cidade',
                          'customer_state':'estado',}, inplace=True)
# print(dim_clientes.dtypes)
```

**Na tabela de items, fiz um join com a tabela de pagamentos, já que todo item possui um preço e precisa de um pagamento para se tornar uma venda**

Também renomeei as colunas.

```py
#Alterações da tabela dim_itens
dim_item = olist_ordem_items.merge(olist_pagamentos,
                            left_on='order_id',
                            right_on='order_id',
                            how='left')
dim_item.rename(columns={'order_item_id':'item_id',
                          'seller_id':'id_vendedor',
                          'shipping_limit_date':'data_limite_envio',
                          'price':'preco',
                          'freight_value':'valor_frete'}, inplace=True)
# print(dim_item.dtypes)
```

**Na tabela de produtos, normalizei os dados passando as colunas de dimensão para apenas uma coluna *dimensoes***

Dropei alguns dados desnecessários para responder as perspectivas ,e renomeei algumas colunas

```py
#Alterações da tabela dim_produtos
dim_produto = olist_produtos

#Junção de todas as dimensoes em uma unica coluna "dimensoes"
dim_produto['dimensoes'] = dim_produto.apply(lambda row: '{:.0f}x{:.0f}x{:.0f}'.format(row['product_width_cm'], row['product_height_cm'], row['product_length_cm']), axis=1)

#Julguei esses dados como contribuições desnecessárias para responder as perspectivas.
dim_produto = dim_produto.drop(['product_width_cm', 
                                'product_height_cm', 
                                'product_length_cm', 
                                'product_description_lenght',   
                                'product_photos_qty',
                                'product_name_lenght',], 
                                axis=1) 
dim_produto.rename(columns={'product_category_name':'categoria',
                            'product_weight_g':'peso_produto'}, inplace=True)
# print(dim_produto.dtypes)
```

**Depois de normalizar os daodos, criei uma função fazendo um try except para verificar se os dados são inseridos de forma correta no banco**

Caso os dados não sejam inseridos, é feito um rollback e apagado todos os dados.

```py
# Verificando se os dados foram inseridos corretamente

def insert_data(data_frame, table_name, schema_name):
    try:
        data_frame.to_sql(name=table_name, con=conn, if_exists='append', index=False, schema=schema_name)
        conn.commit()
        return print(f'Dados inseridos com sucesso na tabela {schema_name}.{table_name}')
    except Exception as e:
        conn.rollback()
        return print(f'Erro ao inserir dados na tabela {schema_name}.{table_name}: {str(e)}')


#Inserindo dados na tabela
insert_data(order_reviews, 'dim_produto_review', 'ecommerce')
insert_data(fato_vendas, 'fato_vendas', 'ecommerce')
insert_data(dim_clientes, 'dim_cliente', 'ecommerce')
insert_data(dim_item, 'dim_item', 'ecommerce')
insert_data(dim_produto, 'dim_produto', 'ecommerce')
```

- [ETL COMPLETO](/PROJETO%20I/projeto-1/python_etl/etl.py)