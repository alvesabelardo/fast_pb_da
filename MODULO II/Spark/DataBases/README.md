# Comandos

spark.sql("create databases desp")          (criando o banco de dados 'desp')

spark.sql("show databases").show()       (mostrar databases)

spark.sql("use desp").show()            (usando o banco de dados criado)

arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"        (criando o schema)

despachantes = spark.read.csv("/home/abel/download/despachantes.csv", header=False, schema=arqschema)       (lendo o arquivo)

despachantes.write.saveAsTable("Despachantes")      (escrevendo como tabela e salvando)

spark.sql("select * from despachantes").show()      (realizando consulta da tabela)

spark.sql("show tables").show()             (mostrando todas as tabelas)

despachantes.write.format("parquet").save("/home/abel/desparquet")      (salvando no diretório (não gerenciada pelo spark))

park.sql("show create table Despachantes").show(truncate=False)     (vê como a tabela foi criada na sintax SQL)

truncate=False -> Os dados não são truncados no console.

> +-------------------------------------------------------------------------------------------------------------------------------------------------------------+
> |createtab_stmt                                                                                                                                               |
> +-------------------------------------------------------------------------------------------------------------------------------------------------------------+
> |CREATE TABLE spark_catalog.desp.Despachantes (id INT,
>                                               nome STRING,
>                                               status STRING,
>                                               cidade STRING,
>                                               vendas INT,
>                                               data STRING)
>                                               USING parquet
> +-------------------------------------------------------------------------------------------------------------------------------------------------------------+

spark.catalog.listTables()          (listar tabelas do DB)

> [Table(name='despachantes', catalog='spark_catalog', namespace=['desp'], description=None, tableType='MANAGED', isTemporary=False), Table(name='despachantes_ng', catalog='spark_catalog', namespace=['desp'], description=None, tableType='EXTERNAL', isTemporary=False)]

## Views

despachantes.createOrReplaceTempView("Despachantes_view1")      (Criando view temporaria)

spark.sql("select * from Despachantes_view1").show()

despachantes.createOrReplaceGlobalTempView("Despachantes_view2")        (Criando view global)

spark.sql("select * from global_temp.Despachantes_view2").show()         (Consultando view global)

spark.sql("CREATE OR REPLACE TEMP VIEW DESP_VIEW AS select * from despachantes")    (Criando view sintax SQL)

>DataFrame[]

## Exemplos Spark Sintax X SQL Sintax

spark.sql("select nome, vendas from Despachantes").show()

despachantes.select("nome","vendas").show()

|               nome       | vendas |
| ----------------------- | ------- |
| Carminda Pestana       | 23      |
| Deolinda Vilela        | 34      |
| Emídio Dornelles       | 34      |
| Felisbela Dornelles    | 36      |
| Graça Ornellas         | 12      |
| Matilde Rebouças       | 22      |
| Noêmia   Orriça        | 45      |
| Roque Vásquez          | 65      |
| Uriel Queiroz          | 54      |
| Viviana Sequeira       | 0       |



spark.sql("select nome, vendas from Despachantes where vendas > 20").show()

despachantes.select("nome","vendas").where(Func.col("vendas") > 20).show()

|               nome       | vendas |
| ----------------------- | ------- |
| Carminda Pestana       | 23      |
| Deolinda Vilela        | 34      |
| Emídio Dornelles       | 34      |
| Felisbela Dornelles    | 36      |
| Matilde Rebouças       | 22      |
| Noêmia   Orriça        | 45      |
| Roque Vásquez          | 65      |
| Uriel Queiroz          | 54      |

spark.sql("select cidade, sum(vendas) from Despachantes group by cidade order by 2 desc").show()
despachantes.groupby("cidade").agg(sum("vendas")).orderBy(Func.col("sum(vendas)").desc()).show()

|   cidade      | sum(vendas) |
| ------------- | ----------- |
| Porto Alegre  | 223         |
| Santa Maria   | 68          |
| Novo Hamburgo | 34          |

## Atividades

#### 1 - Importar Arquivos e Criar Tabelas

*Importando arquivos*

clientes = spark.read.load("/home/abel/download/Atividades/Clientes.parquet")

vendas = spark.read.load("/home/abel/download/Atividades/Vendas.parquet")

itensVendas = spark.read.load("/home/abel/download/Atividades/ItensVendas.parquet")

produtos = spark.read.load("/home/abel/download/Atividades/Produtos.parquet")

vendedores = spark.read.load("/home/abel/download/Atividades/Vendedores.parquet")


*Criando database*

spark.sql("create database VendasVarejo").show()

spark.sql("use VendasVarejo")

*Salvando DataFrames*

clientes.write.saveAsTable("clientes")

vendas.write.saveAsTable("vendas")

itensVendas.write.saveAsTable("itensVendas")

produtos.write.saveAsTable("produtos")

vendedores.write.saveAsTable("vendedores")

spark.sql("show tables").show()

|   namespace    |  tableName  | isTemporary |
| --------------- | ----------- | ----------- |
| vendasvarejo    | clientes    | false       |
| vendasvarejo    | itensvendas | false       |
| vendasvarejo    | produtos    | false       |
| vendasvarejo    | vendas      | false       |
| vendasvarejo    | vendedores  | false       |


spark.sql("select * from produtos").show()

| ProdutoID | Produto                                 | Preco   |
| ---------  | --------------------------------------- | -------  |
| 1         | Bicicleta Aro 29 Modelo XT1000         | 8.852,00 |
| 2         | Bicicleta Altools com Suspensão        | 9.201,00 |
| 3         | Bicicleta Gts Adventure 3000           | 4.255,00 |
| 4         | Bicicleta Trinc CrossMax 5000          | 7.658,00 |
| 5         | Bicicleta Gometws Speed Pro            | 2.966,00 |
| 6         | Bicicleta Gometws Mountain Pro         | 2.955,00 |
| 7         | Capacete Gometws Extreme               | 155,00  |
| 8         | Luva De Ciclismo Modelo Pro             | 188,00  |
| 9         | Bermuda Predactor Pro                  | 115,00  |
| 10        | Camiseta Predactor Pro                  | 135,00  |


#### 2 - Criar uma consulta mostrando cada item vendido: Nome do Cliente, Data da Venda, Produto, Vendedor, e Valor Total.

spark.sql("use vendasvarejo")

spark.sql("select c.cliente, v.data, p.produto, vd.vendedor, iv.valortotal from itensvendas iv

inner join produtos p on (p.produtoid = iv.produtoid)

inner join vendas v on (v.vendasid = iv.vendasid) 

inner join vendedores vd on (vd.vendedorid = v.vendedorid) 

inner join clientes c on (c.clienteid = v.clienteid)").show()


|         Cliente          |    Data    |           Produto            |      Vendedor      | Valor Total |
|--------------------------|------------|-----------------------------|--------------------|-------------|
| Humberto Almeida        | 28/12/2019 | Bicicleta Altools Aro 29   | Iberê Lacerda      | 18,402.0    |
| Bárbara Magalhães       | 15/12/2020 | Bicicleta Altools Aro 29   | Hélio Liberato     | 18,402.0    |
| Artur Macedo            | 22/12/2018 | Bicicleta Trinc CrossMax 5000 | Hélio Liberato     | 13,784.4    |
| Dinarte Tabalipa        | 1/12/2020  | Bicicleta Trinc CrossMax 5000 | Daniel Pirajá      | 13,018.6    |
| Humberto Lemes          | 12/12/2019 | Bicicleta Altools Aro 29   | Simão Rivero       | 14,077.54   |
| Antão Corte-Real        | 16/11/2018 | Bicicleta Altools Aro 29   | Iberê Lacerda      | 16,561.8    |
| Cândido Sousa do ...   | 10/11/2018 | Bicicleta Altools Aro 29   | Daniel Pirajá      | 16,561.8    |
| Brígida Gusmão          | 23/12/2019 | Bicicleta Altools Aro 29   | Hélio Liberato     | 9,201.0     |
| Antão Corte-Real        | 16/11/2018 | Bicicleta Aro 29 Modelo XT1000 | Iberê Lacerda      | 15,933.6    |
| Gertrudes Rabello       | 5/9/2019   | Bicicleta Altools Aro 29   | Hélio Liberato     | 16,561.8    |
| Adélio Lisboa           | 23/11/2019 | Bicicleta Trinc CrossMax 5000 | Hélio Liberato     | 11,716.74   |
| Francisca Ramallo       | 9/12/2020  | Bicicleta Altools Aro 29   | Jéssica Castelão   | 8,280.9     |
| Adélio Lisboa           | 5/12/2019  | Bicicleta Altools Aro 29   | Armando Lago      | 9,201.0     |
| Brígida Gusmão          | 23/12/2019 | Bicicleta Aro 29 Modelo XT1000 | Hélio Liberato     | 7,524.2     |
| Antão Corte-Real        | 15/10/2020 | Bicicleta Altools Aro 29   | Armando Lago      | 16,561.8    |
| Cândido Sousa do ...   | 24/11/2018 | Bicicleta Trinc CrossMax 5000 | Jéssica Castelão   | 13,018.6    |
| Adélio Lisboa           | 5/12/2019  | Bicicleta Trinc CrossMax 5000 | Armando Lago      | 7,658.0     |
| Adolfo Patrício         | 7/11/2020  | Bicicleta Gts Adventure 3000 | Hélio Liberato     | 8,510.0     |
| Brígida Gusmão          | 25/12/2019 | Bicicleta Aro 29 Modelo XT1000 | Godo Capiperibe    | 6,771.78    |
| Adélio Lisboa           | 29/9/2020  | Bicicleta Aro 29 Modelo XT1000 | Jéssica Castelão   | 13,543.56   |

>only showing top 20 rows

