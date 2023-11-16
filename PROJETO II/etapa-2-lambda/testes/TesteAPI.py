import requests
import pandas as pd
import json

api_key = "77f196738eb51a0a35a6211cc16a099e"
movie_ids = pd.read_csv('id_movies.csv')

df_complementos = []

for index, row in movie_ids.iterrows():
    movie_id = row['Movie ID']
    url_details = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}&language=en-US"
    response = requests.get(url_details)
    data = response.json()
    if 'original_title' in data:
        df = {
            'imdb_id': data['imdb_id'],
            'lingua_original': data['original_language'],
            'popularidade': data['popularity'],
            'generos': [genre['name'] for genre in data['genres']],
            'produtoras': [company['name'] for company in data['production_companies']],
            'orcamento': data['budget'],
            'receita': data['revenue'],
        }

        df_complementos.append(df)
    else:
        print("Chave 'original_title' não encontrada nos detalhes do filme.")

try:
    with open('./etapa-2-lambda/data/complementos_filmes.json', 'w') as json_file:
        json.dump(df_complementos, json_file)
    print("Dados salvos com sucesso.")
except Exception as e:
    print(f"Erro ao salvar os dados em JSON: {str(e)}")