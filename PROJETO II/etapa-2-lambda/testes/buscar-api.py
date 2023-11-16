from dotenv import load_dotenv
import requests
import os
import pandas as pd
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Filtrar filmes em que Leonardo DiCaprio atuou
spark = SparkSession.builder.appName("Teste1").getOrCreate()
df = spark.read.csv('./input-local/app/dados/movies.csv', sep='|', header=True, inferSchema=True)
filter_filme_analisar = df.filter(col("nomeArtista") == "Leonardo DiCaprio").select("id").withColumnRenamed("id", "imdb_id")

imdb_ids = filter_filme_analisar.select("imdb_id").rdd.map(lambda x: x[0]).collect()
api_key = os.getenv("API_KEY")
id_list = []

for imdb_id in imdb_ids:
    url_details = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={api_key}&external_source=imdb_id&language=en-US"
    response = requests.get(url_details)
    data = response.json()

    if 'movie_results' in data and data['movie_results']:
        movie_data = data['movie_results'][0]
        movie_id = movie_data.get('id')
        id_list.append({'Movie ID': movie_id})
    else:
        print(f"Não foi possível obter informações para o IMDb ID: {imdb_id}")

df = pd.DataFrame(id_list)

# Salvar DataFrame em um arquivo CSV
df.to_csv('./etapa-2-lambda/data/filmes_ldc_ids.csv', index=False)
print("Arquivo CSV criado com sucesso.")