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
    connection_type="s3",
    connection_options={"paths": [source_file]},
    format="json",
    format_options={
        "multiline": True,
        }
)

df = dyf.toDF()

#Seleciona as estruturas da coluna receita, retira as nulas, junta as tabelas 
df_receita_long = df.select(col('receita.long').alias('receita_long'), 'imdb_id')
df_receita_long = df_receita_long.filter(col('receita_long').isNotNull())

df_receita_int = df.select(col('receita.int').alias('receita_int'), 'imdb_id')
df_receita_int = df_receita_int.filter(col('receita_int').isNotNull())
df_receita_int = df_receita_int.withColumn('receita_int', col('receita_int').cast('long'))

#retira a coluna receita antiga 
df = df.drop('receita')

# Junção dos DataFrames e cria a nova coluna 'receita' com os dados formatados em long
df_receita = df_receita_int.join(df_receita_long, 'imdb_id', 'outer')
df_receita = df_receita.withColumn('receita', coalesce(col('receita_long'), col('receita_int')))
df_receita = df_receita.drop('receita_int').drop('receita_long')

# Junta receita com a tabela antiga
df = df_receita.join(df, 'imdb_id', 'outer')

# Tabela genero
df_genero = df.select(explode("generos").alias("genero")).distinct().withColumn("id_genero", monotonically_increasing_id())

# Tabela filme_genero
df_filme_genero = df.select('imdb_id', explode('generos').alias('genero'))
df_filme_genero = df_filme_genero.join(df_genero, 'genero', 'right_outer').drop(col('genero')).withColumn("id_filme_genero", monotonically_increasing_id()).orderBy('id_filme_genero')

# Tabela produtora
df_produtora = df.select(explode("produtoras").alias("produtora")).distinct().withColumn('id_produtora', monotonically_increasing_id())

# Tabela filme_produtora
df_filme_produtora = df.select('imdb_id', explode('produtoras').alias('produtora'))
df_filme_produtora = df_filme_produtora.join(df_produtora, 'produtora', 'right_outer').drop(col('produtora')).withColumn("id_filme_produtora", monotonically_increasing_id()).orderBy('id_filme_produtora')

df = df.drop(col('generos'))
df = df.drop(col('produtoras'))
# não utilizei
df = df.drop(col('lingua_original'))

# df.show(5) #filme
df = df.repartition(1)
df = DynamicFrame.fromDF(df, glueContext,"dynamic_frame")
# df_genero.show(5) #genero
df_genero = df_genero.repartition(1)
df_genero = DynamicFrame.fromDF(df_genero, glueContext,"dynamic_frame")
# df_filme_genero.show(5) #filme_genero
df_filme_genero = df_filme_genero.repartition(1)
df_filme_genero = DynamicFrame.fromDF(df_filme_genero, glueContext,"dynamic_frame")
# df_produtora.show(5) #produtora
df_produtora = df_produtora.repartition(1)
df_produtora = DynamicFrame.fromDF(df_produtora, glueContext,"dynamic_frame")
# df_filme_produtora.show(5) #filme_produtora
df_filme_produtora = df_filme_produtora.repartition(1)
df_filme_produtora = DynamicFrame.fromDF(df_filme_produtora, glueContext,"dynamic_frame")


glueContext.write_dynamic_frame.from_options(
    frame= df, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/19/dados_filme"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame= df_genero, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/19/genero"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame= df_filme_genero, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/19/filme_genero"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame= df_produtora, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/19/produtora"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame= df_filme_produtora, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/19/filme_produtora"},
    format = 'parquet'
    )

job.commit()