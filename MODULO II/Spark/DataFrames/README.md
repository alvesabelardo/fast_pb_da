# Comandos

-> start-master.sh      (iniciar spark na porta 8080)

-> pyspark      (iniciar terminal)

-> gedit "arquivo"  (abrir leitor e editor de arquivo)

-> JavaWordCount      (Contador de palavras)

## Execuções Console

from pyspark.sql import SparkSession

df1 = spark.createDataFrame([('Pedro', 10), ('Maria', 20), ('José',40)])

df1.show()
+-----+---+                                                                     
|   _1| _2|
+-----+---+
|Pedro| 10|
|Maria| 20|
| José| 40|
+-----+---+

schema = 'Id INT, Nome STRING'      (Definindo o schema)

dados = [[1,"Pedro"], [2,"Maria"]]

df2 = spark.createDataFrame(dados, schema)

df2.show()

+---+-----+
| Id| Nome|
+---+-----+
|  1|Pedro|
|  2|Maria|
+---+-----+

from pyspark.sql.functions import sum 

schema2 = 'Produtos STRING, Vendas INT'

vendas = [['Caneta', 10], ['Lápis', 20], ['Caneta', 40]]

df3 = spark.createDataFrame(vendas,schema2)

df3.show()

+--------+------+
|Produtos|Vendas|
+--------+------+
|  Caneta|    10|
|   Lápis|    20|
|  Caneta|    40|
+--------+------+

df3.show(1)

+--------+------+
|Produtos|Vendas|
+--------+------+
|  Caneta|    10|
+--------+------+

only showing top 1 row

agrupado = df3.groupBy('Produtos').agg(sum("Vendas"))

agrupado.show()

+--------+-----------+
|Produtos|sum(Vendas)|
+--------+-----------+
|  Caneta|         50|
|   Lápis|         20|
+--------+-----------+

from pyspark.sql.functions import expr

df3.select("Produtos","Vendas", expr("Vendas * 0.2")).show()        (cria uma coluna com as funções do 'expr')

+--------+------+--------------+
|Produtos|Vendas|(Vendas * 0.2)|
+--------+------+--------------+
|  Caneta|    10|           2.0|
|   Lápis|    20|           4.0|
|  Caneta|    40|           8.0|
+--------+------+--------------+


from pyspark.sql.types import *

arqschema = 'id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING'

despachantes = spark.read.csv("/home/abel/download/despachantes.csv", header=False, schema=arqschema)

despachantes.show()

+---+-------------------+------+-------------+------+----------+
| id|               nome|status|       cidade|vendas|      data|
+---+-------------------+------+-------------+------+----------+
|  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
|  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
|  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
|  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
|  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
|  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
|  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
|  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
| 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
+---+-------------------+------+-------------+------+----------+

from pyspark.sql import SparkSession

from pyspark.sql.functions import sum

from pyspark.sql.functions import expr

from pyspark.sql.types import *

from pyspark.sql import functions as Func

arqschema = 'id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING'

despachantes = spark.read.csv("/home/abel/download/despachantes.csv", header=False, schema=arqschema)

despachantes.select("id","nome","vendas").where(Func.col("vendas") > 20).show()

+---+-------------------+------+
| id|               nome|vendas|
+---+-------------------+------+
|  1|   Carminda Pestana|    23|
|  2|    Deolinda Vilela|    34|
|  3|   Emídio Dornelles|    34|
|  4|Felisbela Dornelles|    36|
|  6|   Matilde Rebouças|    22|
|  7|    Noêmia   Orriça|    45|
|  8|      Roque Vásquez|    65|
|  9|      Uriel Queiroz|    54|
+---+-------------------+------+

despachantes.select("id","nome","vendas").where((Func.col("vendas") > 20) & (Func.col("vendas") < 40)).show()

+---+-------------------+------+
| id|               nome|vendas|
+---+-------------------+------+
|  1|   Carminda Pestana|    23|
|  2|    Deolinda Vilela|    34|
|  3|   Emídio Dornelles|    34|
|  4|Felisbela Dornelles|    36|
|  6|   Matilde Rebouças|    22|
+---+-------------------+------+

novodf = despachantes.withColumnRenamed("nome","nomes")         (trocando o "nome" da tabela por "nomes")

novodf.columns

>['id', 'nomes', 'status', 'cidade', 'vendas', 'data']

despachantes2 = despachantes.withColumn("data2", to_timestamp(Func.col("data"), "yyyy-MM-dd"))          (adicionando uma coluna data2 como data)

despachantes2.schema

>StructType([StructField('id', IntegerType(), True), StructField('nome', StringType(), True), StructField('status', StringType(), True), StructField('cidade', StringType(), True), StructField('vendas', IntegerType(), True), StructField('data', StringType(), True), StructField('data2', TimestampType(), True)])

despachantes2.select(year("data")).show()       (selecionando anos, por data)
>+----------+
>|year(data)|
>+----------+
>|      2020|
>|      2020|
>|      2020|
>|      2020|
>|      2020|
>|      2019|
>|      2019|
>|      2020|
>|      2018|
>|      2020|
>+----------+


despachantes2.select(year("data")).distinct().show()        (distinct exibe os anos apenas uma vez)

+----------+
|year(data)|
+----------+
|      2018|
|      2019|
|      2020|
+----------+

despachantes2.select("nome", year("data")).orderBy("nome").show()       (buscando nome, apenas o ano na data e ordenando por nome)

+-------------------+----------+
|               nome|year(data)|
+-------------------+----------+
|   Carminda Pestana|      2020|
|    Deolinda Vilela|      2020|
|   Emídio Dornelles|      2020|
|Felisbela Dornelles|      2020|
|     Graça Ornellas|      2020|
|   Matilde Rebouças|      2019|
|    Noêmia   Orriça|      2019|
|      Roque Vásquez|      2020|
|      Uriel Queiroz|      2018|
|   Viviana Sequeira|      2020|
+-------------------+----------+

despachantes2.select("data").groupBy(year("data")).count().show()       (buscando a data, ordenando por ano e contando qtds vezes o ano aparece)

+----------+-----+
|year(data)|count|
+----------+-----+
|      2018|    1|
|      2019|    2|
|      2020|    7|
+----------+-----+

despachantes2.select(Func.sum("vendas")).show()         (somando as vendas)

+-----------+
|sum(vendas)|
+-----------+
|        325|
+-----------+

despachantes.groupBy("cidade").agg(sum("vendas")).orderBy(Func.col("sum(vendas)").desc()).show()    (agrupando por cidade, agredando e somando vendas e ordedando pela coluna sum(vendas))

+-------------+-----------+
|       cidade|sum(vendas)|
+-------------+-----------+
| Porto Alegre|        223|
|  Santa Maria|         68|
|Novo Hamburgo|         34|
+-------------+-----------+

despachantes.filter(Func.col("nome") == "Deolinda Vilela").show()       (filtrando nome)

+---+---------------+------+-------------+------+----------+
| id|           nome|status|       cidade|vendas|      data|
+---+---------------+------+-------------+------+----------+
|  2|Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
+---+---------------+------+-------------+------+----------+

despachantes.orderBy(Func.col("vendas").desc()).show()      (ordenando por vendas)

+---+-------------------+------+-------------+------+----------+
| id|               nome|status|       cidade|vendas|      data|
+---+-------------------+------+-------------+------+----------+
|  8|      Roque Vásquez| Ativo| Porto Alegre|    65|2020-03-05|
|  9|      Uriel Queiroz| Ativo| Porto Alegre|    54|2018-05-05|
|  7|    Noêmia   Orriça| Ativo|  Santa Maria|    45|2019-10-05|
|  4|Felisbela Dornelles| Ativo| Porto Alegre|    36|2020-02-05|
|  2|    Deolinda Vilela| Ativo|Novo Hamburgo|    34|2020-03-05|
|  3|   Emídio Dornelles| Ativo| Porto Alegre|    34|2020-02-05|
|  1|   Carminda Pestana| Ativo|  Santa Maria|    23|2020-08-11|
|  6|   Matilde Rebouças| Ativo| Porto Alegre|    22|2019-01-05|
|  5|     Graça Ornellas| Ativo| Porto Alegre|    12|2020-02-05|
| 10|   Viviana Sequeira| Ativo| Porto Alegre|     0|2020-09-05|
+---+-------------------+------+-------------+------+----------+

#### ARMAZENAR DADOS 

despachantes.write.format("parquet").save("/home/abel/dfimportparquet")

despachantes.write.format("csv").save("/home/abel/dfimportcsv")

despachantes.write.format("json").save("/home/abel/dfimportjson")

#### IMPORTAR DADOS

par = spark.read.format("parquet").load("/home/abel/dfimportparquet/despachantes.parquet")

## Atividades

### 1- Crie uma consulta que mostre Nome, Estado e Status nessa ordem

clientes.select("Cliente","Estado","Status").show(10)

+--------------------+------+--------+
|             Cliente|Estado|  Status|
+--------------------+------+--------+
|Adelina Buenaventura|    RJ|  Silver|
|        Adelino Gago|    RJ|  Silver|
|     Adolfo Patrício|    PE|  Silver|
|    Adriana Guedelha|    RO|Platinum|
|       Adélio Lisboa|    SE|  Silver|
|       Adérito Bahía|    MA|  Silver|
|       Aida Dorneles|    RN|  Silver|
|   Alarico Quinterno|    AC|  Silver|
|    Alberto Cezimbra|    AM|  Silver|
|    Alberto Monsanto|    RN|    Gold|
+--------------------+------+--------+
only showing top 10 rows

### 2- Crie uma consulta que mostre apenas os clientes "Platinum" e "Gold"

clientes.filter((Func.col("Status") == "Gold") | (Func.col("Status") == "Platinum")).show()

+---------+-------------------+------+------+--------+
|ClienteID|            Cliente|Estado|Genero|  Status|
+---------+-------------------+------+------+--------+
|        4|   Adriana Guedelha|    RO|     F|Platinum|
|       10|   Alberto Monsanto|    RN|     M|    Gold|
|       28|      Anna Carvajal|    RS|     F|    Gold|
|       49|      Bento Quintão|    SP|     M|    Gold|
|       68|      Carminda Dias|    AM|     F|    Gold|
|       83|      Cláudio Jorge|    TO|     M|    Gold|
|      121|    Dionísio Saltão|    PR|     M|    Gold|
|      166|   Firmino Meireles|    AM|     M|    Gold|
|      170|      Flor Vilanova|    CE|     M|Platinum|
|      220|Honorina Villaverde|    PE|     F|    Gold|
|      230|    Ibijara Botelho|    RR|     F|Platinum|
|      237|  Iracema Rodríguez|    BA|     F|    Gold|
|      247|         Joana Ataí|    GO|     F|Platinum|
+---------+-------------------+------+------+--------+

### 3- Demonstre cada Status de Cliente representa de vendas:

vendas.join(clientes, vendas.ClienteID == clientes.ClienteID).groupBy(clientes.Status).agg(sum("Total")).orderBy(Func.col("sum(Total)").desc()).show()

(Faz um join do ID do Cliente que é FK na tabela vendas, agroupa pelo Status da tabela cliente e agrega a soma do total, depois ordena em ordem decrescente a coluna "Total")

+--------+------------------+
|  Status|        sum(Total)|
+--------+------------------+
|  Silver|        3014291.36|
|    Gold|27286.690000000002|
|Platinum|          12584.68|
+--------+------------------+