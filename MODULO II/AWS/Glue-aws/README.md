# Execuções Glue 

### 1 - Ler Arquivo

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

df = glueContext.create_dynamic_frame.from_options(
    "s3", {
        "paths": [source_file]
    },
    "csv", {
        "withHeader": True,
        "separator": ","
    }
)



### 2 - Imprimir Schema do DataFrame

df.printSchema()

>root
>|-- nome: string
>|-- sexo: string
>|-- total: string
>|-- ano: string


### 3 - Alterar os Valores para maiúsculo 

upper = udf(lambda x: x.upper(), StringType())
ToDataFrame = df.toDF()
df2 = ToDataFrame.withColumn("nome", upper(col("nome")))

>{"nome":"MARY","total":"7065"}
>{"nome":"ANNA","total":"2604"}
>{"nome":"EMMA","total":"2003"}
>{"nome":"ELIZABETH","total":"1939"}
>{"nome":"MINNIE","total":"1746"}
>{"nome":"MARGARET","total":"1578"}

### 4 - Imprimir contagem de linhas

count_result = df2.count()
print(f"Number of lines in DataFrame: {count_result}")

>Number of lines in DataFrame: 1825433

### 5 - Imprimir a contagem  de nomes, agrupando os dados do data frame pelas colunas ano e sexo, e ordenar os dados de forma que o ano mais recente apareça primeiro no DataFrame

agrupar = df2.groupBy("ano", "sexo").agg(count("nome").alias("count")).orderBy("ano", desc("count"))
agrupar.show()

| ano  | sexo | count |
|------|------|-------|
| 1880 |  M   | 1058  |
| 1880 |  F   |  942  |
| 1881 |  M   |  997  |
| 1881 |  F   |  938  |
| 1882 |  M   | 1099  |
| 1882 |  F   | 1028  |
| 1883 |  F   | 1054  |
| 1883 |  M   | 1030  |
| 1884 |  F   | 1172  |
| 1884 |  M   | 1125  |
| 1885 |  F   | 1197  |
| 1885 |  M   | 1097  |
| 1886 |  F   | 1282  |
| 1886 |  M   | 1110  |
| 1887 |  F   | 1306  |
| 1887 |  M   | 1067  |
| 1888 |  F   | 1474  |
| 1888 |  M   | 1177  |
| 1889 |  F   | 1479  |
| 1889 |  M   | 1111  |
only showing top 20 rows

### 6 - Apresentar qual foi o nome feminino com mais registro e quando ocorreu

![Registros Femininos](/img/registros_F.png)

### 7 - Apresentar qual foi o nome Masculino com mais registro e quando ocorreu

![Registros Masculinos](/img/registros_M.png)

### 8 - Apresentar o total de registros (masculino e femininos) para cada ano e ordenar de forma crescente

| ano  | total_registros |
|------|------------------|
| 1880 | 2000             |
| 1881 | 1935             |
| 1882 | 2127             |
| 1883 | 2084             |
| 1884 | 2297             |
| 1885 | 2294             |
| 1886 | 2392             |
| 1887 | 2373             |
| 1888 | 2651             |
| 1889 | 2590             |

only showing top 10 rows

### 9 - Escrever o conteúdo em Maiusculo no S3 em JSON e particionar por sexo e ano 

![Particionamento por sexo](/img/particao-sexo.png)
![Particionamento por ano](/img/particao-ano.png)
![Salvando em json e UpperCase](/img/salvos-json-uppercase.png.png)

### 10 - Código Full

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

df = glueContext.create_dynamic_frame.from_options(
    "s3", {
        "paths": [source_file]
    },
    "csv", {
        "withHeader": True,
        "separator": ","
    }
)
#Print Schema
df.printSchema()

#UpperCase
upper = udf(lambda x: x.upper(), StringType())
ToDataFrame = df.toDF()
df2 = ToDataFrame.withColumn("nome", upper(col("nome")))

#Count Lines
count_result = df2.count()
print(f"Number of lines in DataFrame: {count_result}")

#Agrupa por ano e sexo
agrupar = df2.groupBy("ano", "sexo").agg(count("nome").alias("count")).orderBy("ano", desc("count"))
agrupar.show()

#Maximo Registros femininos 
nomes_femininos = df2.filter(df2["sexo"] == "F")
agg_nome_feminino = nomes_femininos.select("nome","total", "ano").orderBy(desc("total"))
result_max_registros_f = agg_nome_feminino.first()
nome_max_registros_f = result_max_registros_f["nome"]
count_max_registros_f = result_max_registros_f["total"]
ano_ocorreu_f = result_max_registros_f["ano"]
print(f"O nome feminino com mais registros foi {nome_max_registros_f} com {count_max_registros_f} registros, no ano {ano_ocorreu_f}")

#Maximo Registros masculinos
nomes_masculinos = df2.filter(df2["sexo"] == "M")
agg_nome_masculino = nomes_masculinos.select("nome","total", "ano").orderBy(desc("total"))
result_max_registros_m = agg_nome_masculino.first()
nome_max_registros_m = result_max_registros_m["nome"]
count_max_registros_m = result_max_registros_m["total"]
ano_ocorreu_m = result_max_registros_m["ano"]
print(f"O nome masculino com mais registros foi {nome_max_registros_m} com {count_max_registros_m} registros, no ano {ano_ocorreu_m}")

#Total registros masculinos e nomes_femininos
total_registros = df2.groupBy("ano").agg(count("*").alias("total_registros")).orderBy(asc("ano"))
total_registros.show(10)

df3 = DynamicFrame.fromDF(df2, glueContext,"dynamic_frame")

#Escreve o DynamicFrame no AWS Glue Catalog com partições
glueContext.write_dynamic_frame.from_catalog(
    frame=df3,
    database="meubanco",
    table_name="frequencia_nomes",
    additional_options={"partitionKeys": ["sexo", "ano"]}
)

job.commit()
