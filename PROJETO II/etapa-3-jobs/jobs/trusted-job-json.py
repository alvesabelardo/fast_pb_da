from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# Define o esquema
custom_schema = StructType([
    StructField("imdb_id", StringType(), True),
    StructField("lingua_original", StringType(), True),
    StructField("popularidade", FloatType(), True),
    StructField("generos", ArrayType(StringType()), True),
    StructField("produtoras", ArrayType(StringType()), True),
    StructField("orcamento", IntegerType(), True),
    StructField("receita", LongType(), True)
])

df = spark.read.option("multiline", "true").schema(custom_schema).option("inferSchema", "true").json(source_file)


# Tabela genero
df_genero = df.select(explode("generos").alias("genero")).distinct()
df_genero = df_genero.withColumn("id_genero", row_number().over(Window.orderBy("genero")))

# Tabela filme_genero
df_filme_genero = df.select('imdb_id', explode('generos').alias('genero'))
df_filme_genero = df_filme_genero.join(df_genero, 'genero', 'right_outer')
df_filme_genero = df_filme_genero.withColumn('id_filme_genero', row_number().over(Window.orderBy('genero'))).drop(col('genero'))

# Tabela produtora
df_produtora = df.select(explode("produtoras").alias("produtora")).distinct()
df_produtora = df_produtora.withColumn("id_produtora", row_number().over(Window.orderBy("produtora")))

# Tabela filme_produtora
df_filme_produtora = df.select('imdb_id', explode('produtoras').alias('produtora'))
df_filme_produtora = df_filme_produtora.join(df_produtora, 'produtora', 'right_outer')
df_filme_produtora = df_filme_produtora.withColumn('id_filme_produtora', row_number().over(Window.orderBy('produtora'))).drop(col('produtora'))


df = df.drop(col('generos'))
df = df.drop(col('produtoras'))
# não utilizei
df = df.drop(col('lingua_original'))

# df.show(100) #filme
df = DynamicFrame.fromDF(df, glueContext,"dynamic_frame")
# df_genero.show(30) #genero
df_genero = DynamicFrame.fromDF(df_genero, glueContext,"dynamic_frame")
# df_filme_genero.show(100) #filme_genero
df_filme_genero = DynamicFrame.fromDF(df_filme_genero, glueContext,"dynamic_frame")
# df_produtora.show(200) #produtora
df_produtora = DynamicFrame.fromDF(df_produtora, glueContext,"dynamic_frame")
# df_filme_produtora.show(200) #filme_produtora
df_filme_produtora = DynamicFrame.fromDF(df_filme_produtora, glueContext,"dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame= df, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/dados_filme"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame= df_genero, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/genero"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame= df_filme_genero, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/filme_genero"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame= df_produtora, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/produtora"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame= df_filme_produtora, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/filme_produtora"},
    format = 'parquet'
    )

job.commit()