# Integração com postgresql

pyspark --jars /opt/spark/jars/postgresql-42.6.0.jar
>(abrindo pyspark com o postgres, lembrar de sempre mover o arquivo para o opt/spark/jars)

resumo  = spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/vendas").option("dbtable","Vendas").option("user", "postgres").option("password","3513").option("driver","org.postgresql.Driver").load()
>(criando variável resumo lendo os dados da tabela vendas)

clientes  = spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/vendas").option("dbtable","Clientes").option("user", "postgres").option("password","3513").option("driver","org.postgresql.Driver").load()
>(criando variável resumo lendo os dados da tabela Clientes)


vendadata = resumo.select("data","total")
>(salvando data e total em um DataFrame vendadata)

vendadata.show()

|    Data    |   Total  |
|------------|----------|
| 2016-01-01 |  8,053.60 |
| 2016-01-01 |  150.40  |
| 2016-01-02 |  6,087.00 |
| 2016-01-02 | 13,828.60 |
| 2016-01-03 | 26,096.66 |
| 2016-01-04 | 18,402.00 |
| 2016-01-06 |  7,524.20 |
| 2016-01-06 | 12,036.60 |
| 2016-01-06 |  2,804.75 |
| 2016-01-06 |  8,852.00 |
| 2016-01-08 | 16,545.25 |
| 2016-01-09 | 11,411.88 |
| 2016-01-10 | 15,829.70 |
| 2016-01-12 |  6,154.36 |
| 2016-01-12 |  3,255.08 |
| 2016-01-13 |  2,901.25 |
| 2016-01-13 | 15,829.70 |
| 2016-01-14 | 16,996.36 |
| 2016-01-14 |  155.00  |
| 2016-01-15 |  131.75  |


vendadata.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/vendas").option("dbtable","Vendadata").option("user", "postgres").option("password","3513").option("driver","org.postgresql.Driver").save()
>(salvando o dataframe como uma tabela)

## PostgreSQL

sudo -u postgres psql
>(rodar o postgres)

\c (database);
>utilizar o banco de dados 

\i "local_do_arquivo"
>inserir arquivos 

\dt
>listar relações
