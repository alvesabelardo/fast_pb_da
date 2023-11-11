# Execuções Glue 

### 1 - Ler Arquivo
```Python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

dyf = glueContext.create_dynamic_frame.from_options(
    "s3", {
        "paths": [source_file]
    },
    "csv", {
        "withHeader": True,
        "separator": ","
    }
)

#Alterando Schema
ToDataFrame = dyf.toDF()
ToDataFrame = ToDataFrame.withColumn("total", col("total").cast("int"))
ToDataFrame = ToDataFrame.withColumn("ano", to_date(col("ano"), "yyyy").cast(DateType()))
```

### 2 - Imprimir Schema do DataFrame

df.printSchema()

```
root
 |-- nome: string 
 |-- sexo: string 
 |-- total: integer 
 |-- ano: date 
```

### 3 - Alterar os Valores para maiúsculo 
```Py
upper = udf(lambda x: x.upper(), StringType())
df = ToDataFrame.withColumn("nome", upper(col("nome")))
```

```Json
{"nome":"MARY","total":"7065"}
{"nome":"ANNA","total":"2604"}
{"nome":"EMMA","total":"2003"}
{"nome":"ELIZABETH","total":"1939"}
{"nome":"MINNIE","total":"1746"}
{"nome":"MARGARET","total":"1578"}
```

### 4 - Imprimir contagem de linhas
```Py
count_result = df.count()
print(f"Number of lines in DataFrame: {count_result}")
```
![Linhas DataFrame](/img/linhas_dataframe.png)
>Number of lines in DataFrame: 1825433

### 5 - Imprimir a contagem  de nomes, agrupando os dados do data frame pelas colunas ano e sexo, e ordenar os dados de forma que o ano mais recente apareça primeiro no DataFrame

```Py
agrupar =  df.groupBy("ano", "sexo").agg(count("*").alias("count")).orderBy("ano", ascending=False)
agrupar.show()
```

|    ano     | sexo | count |
|------------|------|-------|
| 2014-01-01 |   M  | 13977 |
| 2014-01-01 |   F  | 19067 |
| 2013-01-01 |   M  | 14012 |
| 2013-01-01 |   F  | 19191 |
| 2012-01-01 |   M  | 14216 |
| 2012-01-01 |   F  | 19468 |
| 2011-01-01 |   F  | 19540 |
| 2011-01-01 |   M  | 14329 |
| 2010-01-01 |   F  | 19800 |
| 2010-01-01 |   M  | 14241 |
| 2009-01-01 |   F  | 20165 |
| 2009-01-01 |   M  | 14519 |
| 2008-01-01 |   F  | 20439 |
| 2008-01-01 |   M  | 14606 |
| 2007-01-01 |   M  | 14383 |
| 2007-01-01 |   F  | 20548 |
| 2006-01-01 |   M  | 14026 |
| 2006-01-01 |   F  | 20043 |
| 2005-01-01 |   F  | 19175 |
| 2005-01-01 |   M  | 13358 |
only showing top 20 rows

### 6 - Apresentar qual foi o nome feminino com mais registro e quando ocorreu

![Registros Femininos](/img/registros_F.png)

### 7 - Apresentar qual foi o nome Masculino com mais registro e quando ocorreu

![Registros Masculinos](/img/registros_M.png)

### 8 - Apresentar o total de registros (masculino e femininos) para cada ano e ordenar de forma crescente
```Py
total_registros = df.groupBy("ano", "sexo").agg(sum("total").alias("total_registros")).orderBy("ano")
total_registros.show(10)
```

|       ano      | sexo | total_registros |
|--------------|----|---------------|
| 1880-01-01 |   M  |     110491      |
| 1880-01-01 |   F  |      90993      |
| 1881-01-01 |   F  |      91954      |
| 1881-01-01 |   M  |     100745      |
| 1882-01-01 |   M  |     113688      |
| 1882-01-01 |   F  |     107850      |
| 1883-01-01 |   F  |     112321      |
| 1883-01-01 |   M  |     104629      |
| 1884-01-01 |   M  |     114445      |
| 1884-01-01 |   F  |     129022      |


### 9 - Escrever o conteúdo em Maiusculo no S3 em JSON e particionar por sexo e ano 

![Particionamento por sexo](/img/particao-sexo.png)

![Particionamento por ano](/img/particao-ano.png)

![Salvando em json e UpperCase](/img/salvos-json-uppercase.png)

### 10 - Código Full

```Py
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

dyf = glueContext.create_dynamic_frame.from_options(
    "s3", {
        "paths": [source_file]
    },
    "csv", {
        "withHeader": True,
        "separator": ","
    }
)

#Alterando Schema
ToDataFrame = dyf.toDF()
ToDataFrame = ToDataFrame.withColumn("total", col("total").cast("int"))
ToDataFrame = ToDataFrame.withColumn("ano", to_date(col("ano"), "yyyy").cast(DateType()))

# Transformação da coluna "nome" para uppercase
upper = udf(lambda x: x.upper(), StringType())
df = ToDataFrame.withColumn("nome", upper(col("nome")))

# Print schema
ToDataFrame.printSchema()

# Count Lines
count_result = df.count()
print(f"Number of lines in DataFrame: {count_result}")

# Agrupa por ano e sexo
agrupar =  df.groupBy("ano", "sexo").agg(count("*").alias("count")).orderBy("ano", ascending=False)
agrupar.show()

#Maximo Registros femininos 
nomes_femininos = df.filter(df["sexo"] == "F")
agg_nome_feminino = nomes_femininos.select("nome","total", "ano").orderBy(desc("total"))
result_max_registros_f = agg_nome_feminino.first()
nome_max_registros_f = result_max_registros_f["nome"]
count_max_registros_f = result_max_registros_f["total"]
ano_ocorreu_f = result_max_registros_f["ano"]
print(f"O nome feminino com mais registros foi {nome_max_registros_f} com {count_max_registros_f} registros, no ano {ano_ocorreu_f}")

#Maximo Registros masculinos
nomes_masculinos = df.filter(df["sexo"] == "M")
agg_nome_masculino = nomes_masculinos.select("nome","total", "ano").orderBy(desc("total"))
result_max_registros_m = agg_nome_masculino.first()
nome_max_registros_m = result_max_registros_m["nome"]
count_max_registros_m = result_max_registros_m["total"]
ano_ocorreu_m = result_max_registros_m["ano"]
print(f"O nome masculino com mais registros foi {nome_max_registros_m} com {count_max_registros_m} registros, no ano {ano_ocorreu_m}")

#Total registros masculinos e femininos
total_registros = df.groupBy("ano", "sexo").agg(sum("total").alias("total_registros")).orderBy("ano")
total_registros.show(10)

df = DynamicFrame.fromDF(df, glueContext,"dynamic_frame")

# Escreve o DynamicFrame no AWS Glue Catalog com partições
glueContext.write_dynamic_frame.from_catalog(
    frame=df,
    database="meubanco",
    table_name="frequencia_nomes",
    additional_options={"partitionKeys": ["sexo", "ano"]}
)

job.commit()
```