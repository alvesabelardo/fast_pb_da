- É necessário que a tabela "pb_wd" e o schema "ecommerce" estejam criados em seu banco de dados para rodar esse algoritimo

- O psycopg2 é essencial para o funcionamento correto do sqlalchemy

- Dentro da pasta python_etl contém um arquivo .jpg, se trata de um modelo conceitual de como organizei as tabelas fatos e dimensôes

-
    11 #Importando e alterando dados Mongo
    12 db = client.ecommerce.order_reviews
    14 _ = db.find({}, {'_id':0})
    15 order_reviews = pd.DataFrame(_)

- Ao enviar os dados para o MongoDB, a coluna "_id" é criada automaticamente, a linha 14 faz um find excluindo essa coluna

