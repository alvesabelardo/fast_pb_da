from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Filtrar filmes em que Leonardo DiCaprio atuou
spark = SparkSession.builder.appName("Teste1").getOrCreate()
df = spark.read.csv('./input-local/app/dados/movies.csv', sep='|', header=True, inferSchema=True)
filter_filme_analisar = df.filter(col("nomeArtista") == "Leonardo DiCaprio").select("id").withColumnRenamed("id", "imdb_id")

# filter_filme_analisar.write.csv('./etapa-2-lambda/data/filmes_ldc_imdbs.csv', mode='overwrite')

imdb_id_list = filter_filme_analisar.select("imdb_id").rdd.map(lambda x: x[0]).collect()

# Exibir a lista
print(imdb_id_list)