import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

target_path = args['S3_TARGET_PATH']

dyf_dados_filme = glueContext.create_dynamic_frame.from_catalog(database='tl_trusted_zone', table_name='dados_filme')
dyf_filme_genero = glueContext.create_dynamic_frame.from_catalog(database='tl_trusted_zone', table_name='filme_genero')
dyf_filme_produtora = glueContext.create_dynamic_frame.from_catalog(database='tl_trusted_zone', table_name='filme_produtora')
dyf_genero = glueContext.create_dynamic_frame.from_catalog(database='tl_trusted_zone', table_name='genero')
dyf_produtora = glueContext.create_dynamic_frame.from_catalog(database='tl_trusted_zone', table_name='produtora')
dyf_dados_complementares= glueContext.create_dynamic_frame.from_catalog(database='tl_trusted_zone', table_name='tb1_data_csvcsv')

df_dados_filme = dyf_dados_filme.toDF()
filme_genero = dyf_filme_genero.toDF() #Done
filme_produtora = dyf_filme_produtora.toDF() #Done
genero = dyf_genero.toDF() #Done
produtora = dyf_produtora.toDF() #Done
df_dados_complementares = dyf_dados_complementares.toDF()

genero = genero.orderBy('id_genero')
filme_genero = filme_genero.orderBy('id_filme_genero')
produtora = produtora.orderBy('id_produtora')
filme_produtora = filme_produtora.orderBy('id_filme_produtora')

all_data = df_dados_filme.join(df_dados_complementares, 'imdb_id', 'outer') #Todos dados necessários para as outras tabelas

filme = all_data.drop('receita').drop('popularidade').drop('orcamento').drop('media_votos').drop('quantidade_votos') 
filme = filme.withColumnRenamed('nomeArtista', 'ator') #Done

filme_lucro = all_data.drop('popularidade').drop('titulo').drop('lancamento').drop('duracao_minutos').drop('media_votos').drop('quantidade_votos').drop('nomeArtista')
filme_lucro = filme_lucro.withColumn("lucro", (col("receita") - col("orcamento")).cast(IntegerType()))
filme_lucro = filme_lucro.withColumn("percent_lucro", (col("lucro") / col("orcamento")) * 100).withColumn("percent_lucro", col("percent_lucro").cast(FloatType()))
filme_lucro = filme_lucro.withColumn('id_filme_lucro', row_number().over(Window.orderBy('imdb_id'))) #Done

filme_score = all_data.drop('receita').drop('orcamento').drop('titulo').drop('lancamento').drop('duracao_minutos').drop('nomeartista') #Done
filme_score = filme_score.withColumn('id_filme_score', row_number().over(Window.orderBy('imdb_id'))) #Done

filme = DynamicFrame.fromDF(filme, glueContext,"dynamic_frame")
filme_genero = DynamicFrame.fromDF(filme_genero, glueContext,"dynamic_frame")
genero = DynamicFrame.fromDF(genero, glueContext,"dynamic_frame")
filme_produtora = DynamicFrame.fromDF(filme_produtora, glueContext,"dynamic_frame")
produtora = DynamicFrame.fromDF(produtora, glueContext,"dynamic_frame")
filme_score = DynamicFrame.fromDF(filme_score, glueContext,"dynamic_frame")
filme_lucro = DynamicFrame.fromDF(filme_lucro, glueContext,"dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame = filme, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/filme"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame = filme_genero, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/filme_genero"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame = genero, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/genero"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame = filme_produtora, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/filme_produtora"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame = produtora, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/produtora"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame = filme_score, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/filme_score"},
    format = 'parquet'
    )
glueContext.write_dynamic_frame.from_options(
    frame = filme_lucro, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/filme_lucro"},
    format = 'parquet'
    )

job.commit()