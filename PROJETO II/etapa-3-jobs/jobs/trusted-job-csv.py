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
        "separator": "|"
    }

)

drop_colunas = ['tituloPincipal','genero','generoArtista','personagem','anoNascimento','anoFalecimento','profissao','titulosMaisConhecidos']

df = dyf.toDF()
df = df.drop(*drop_colunas)
df = (
    df
    .withColumn('id', col('id').cast('string'))
    .withColumn('tituloOriginal', col('tituloOriginal').cast('string'))
    .withColumn('anoLancamento', col('anoLancamento').cast('int'))
    .withColumn('tempoMinutos', col('tempoMinutos').cast('int'))
    .withColumn('notaMedia', col('notaMedia').cast('float'))
    .withColumn('numeroVotos', col('numeroVotos').cast('int'))
    .withColumn('nomeArtista', col('nomeArtista').cast('string'))
)

df = df.withColumnRenamed('id', 'imdb_id').withColumnRenamed('tituloOriginal', 'titulo').withColumnRenamed('anoLancamento', 'lancamento').withColumnRenamed('tempoMinutos', 'duracao_minutos').withColumnRenamed('notaMedia', 'media_votos').withColumnRenamed('numeroVotos', 'quantidade_votos')

df = df.filter(col('nomeArtista') == 'Leonardo DiCaprio')
df = df.repartition(1)
df = DynamicFrame.fromDF(df, glueContext,"dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame= df ,
    connection_type = 's3',
    connection_options = {"path": target_path},
    format = 'parquet'
    )

job.commit()