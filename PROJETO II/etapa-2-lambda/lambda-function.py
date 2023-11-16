import boto3
import requests
import pandas as pd
from io import StringIO
import csv
import os
import json


def lambda_handler(event, context):

    bucket_name = 'projeto-2'
    filmes = 'data-lake-projeto-2/Raw/input-local/2023/11/14/filmes.csv'
    output_imdbs = 'data-lake-projeto-2/Raw/TMDB/CSV/2023/11/16/imdbs_id.csv'
    output_ids = 'data-lake-projeto-2/Raw/TMDB/CSV/2023/11/16/id_movies.csv'
    output_detalhes = 'data-lake-projeto-2/Raw/TMDB/JSON/2023/11/16/detalhes_filmes.json'
    
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=filmes)
    arquivo = response['Body'].read().decode('utf-8')
    
    #Filtrando filmes em que Leonardo DiCaprio atuou e salvando seus imdb_id's em uma lista
    df = pd.read_csv(StringIO(arquivo), sep='|')
    filter_filme_analisar = df[df['nomeArtista'] == 'Leonardo DiCaprio'][['id']].rename(columns={'id': 'imdb_id'})
    
    imdb_ids = filter_filme_analisar['imdb_id'].tolist()
    imdb_ids_str = '\n'.join(map(str, imdb_ids))
    
    s3_client.put_object(Body=imdb_ids_str, Bucket=bucket_name, Key=output_imdbs)
    # print(f"imdb_ids: {imdb_ids}")
    
    #Buscando o id do filme na api pelos imdb_id's do arquivo local csv
    api_key = os.environ.get('API_KEY')
    id_list = []
    for imdb_id in imdb_ids:
        url_filmes = f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={api_key}&external_source=imdb_id&language=en-US"
        response = requests.get(url_filmes)
        data = response.json()
        
        if 'movie_results' in data and data['movie_results']:
            movie_data = data['movie_results'][0]
            movie_id = movie_data.get('id')
            id_list.append({'id_movie': movie_id})
        else:
            print(f"Erro ao puxar o IMDB: {imdb_id}")
    
        
    df = pd.DataFrame(id_list)
    csv = StringIO()
    df.to_csv(csv, index=False)
    #Salva os ids da API em um csv
    s3_client.put_object(Body=csv.getvalue(), Bucket=bucket_name, Key=output_ids)
    print("Arquivo id_movies.csv criado e enviado para o S3.")
    
    
    response = s3_client.get_object(Bucket=bucket_name, Key=output_ids)
    csv_content = response['Body'].read().decode('utf-8')
    csv_file = StringIO(csv_content)
    movie_ids = pd.read_csv(csv_file)
    
    df_complementos = []
    #Pegando detalhes dos filmes filtrados pelos ids da API na variavel movie_id
    for i, row in movie_ids.iterrows():
        movie_id = row['id_movie']
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
            
        json_data = json.dumps(df_complementos, indent=2)
        
    try:
        s3_client.put_object(Body=json_data, Bucket=bucket_name, Key=output_detalhes, ContentType='application/json')
        print("Dados Complementares em JSON salvos com sucesso.")
        
    except Exception as e:
        print(f"Erro ao salvar os dados em JSON: {str(e)}") 
            
    return {
        'statusCode': 200,
    }
