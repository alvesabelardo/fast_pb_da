## Informações sobre o lambda-function

**A função foi salva exatamente como o código foi rodado no Lambda.**

O código foi desenvolvido a fim de filtrar e trazer informações adicionas da API de todos os filmes em que o ator Leonardo DiCaprio atuou, disponiveis no arquivo *filmes.csv*.

**Algumas métricas utilizadas para fazer a filtragem dos dados a fim de analisá-los são:**

- Relação orçamento e receita.
- Quais filmes tiveram o maior lucro.
- Quais gêneros atuados o ator teve maior popularidade.
- Quais produtoras lucraram mais e com quais filmes.
- Quais filmes/gêneros tiveram a melhor votação.
- Em quais gêneros o ator mais atuou.

### Função

**Primeiramente foram definidas todas as saidas e entradas de arquivos:**

```py
    bucket_name = 'projeto-2'
    filmes = 'data-lake-projeto-2/Raw/input-local/2023/11/14/filmes.csv'
    output_imdbs = 'data-lake-projeto-2/Raw/TMDB/CSV/2023/11/16/imdbs_id.csv'
    output_ids = 'data-lake-projeto-2/Raw/TMDB/CSV/2023/11/16/id_movies.csv'
    output_detalhes = 'data-lake-projeto-2/Raw/TMDB/JSON/2023/11/16/detalhes_filmes.json'
```

- output_imdbs -> Onde os imdb_id dos filmes filtrados serão guardados.
- output_ids -> Onde os id_movie dos filmes filtrados pelo imdb serão guardados.
- output_detalhes -> Onde os detalhes complementares dos filmes filtrados serão guardados em json.


**Filtragem e armazenamento dos dados filtrados**

```py
    df = pd.read_csv(StringIO(arquivo), sep='|')
    filter_filme_analisar = df[df['nomeArtista'] == 'Leonardo DiCaprio'][['id']].rename(columns={'id': 'imdb_id'})
    
    imdb_ids = filter_filme_analisar['imdb_id'].tolist()
    imdb_ids_str = '\n'.join(map(str, imdb_ids))
    
    s3_client.put_object(Body=imdb_ids_str, Bucket=bucket_name, Key=output_imdbs)
```

Posteriormente é realizada uma filtragem dos dados da coluna *'nomeArtista' = 'Leonardo DiCaprio'*, salvando o *id* desses dados e renomeando para *imdb_id*, esses dados são salvos em uma lista e também enviados para o bucket *projeto-2*.

**Requisição do Id_movie na API**

Com os imdbs salvo em uma lista, é possível requisitar os movie_id dos filmes filtrados, que posteriormente traram as informações necessárias para complementar os dados do arquivo csv

```py
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

    s3_client.put_object(Body=csv.getvalue(), Bucket=bucket_name, Key=output_ids)
    print("Arquivo id_movies.csv criado e enviado para o S3.")
```

- A API_KEY setada como váriavel de ambiente, é passada para uma váriavel, e é criado uma lista de id's.

- Para cada imdb_id o loop faz uma requisição na API e salva somente o *movie_id* dos filmes filtrados.

- Depois, o arquivo se torna um dataframe para ser salvo como arquivo csv, a váriavel *csv* faz a conversão de uma string para arquivo e posteriormente a chamada *s3_client.put_object* salva esse arquivo no *output_ids* no bucket do *projeto-2*


**Dessa vez, o arquivo é chamado pelo get_object no bucket, e tratado para ser lido como uma string**

- Na váriavel *response* arquivo é requisitado do bucket e lido na váriavel *csv_content*.

- Novamente utilizo o *StringIO*, mas para transformar de arquivo, para string, fazendo com que seja possível fazer a leitura com pandas.

```py
    response = s3_client.get_object(Bucket=bucket_name, Key=output_ids)
    csv_content = response['Body'].read().decode('utf-8')
    csv_file = StringIO(csv_content)
    movie_ids = pd.read_csv(csv_file)
    
    df_complementos = []
```

- Com o arquivo legível e lista definida, podemos fazer a requisição de dados na API.

**Como os dados devem ser complementares, optei por trazer somente o necessário, para não repetir os dados do arquivo csv**

- Para cada item do movie_ids é feita uma requisição na API, os dados são salvos em json no *data*.

- Se a chave *original_title* estiver presente nos dados, salvo os dados escolhidos no dicionário *df* e são adicionados em uma lista, *df_complementos* para que sejam separados corretamente.

- Depois, os dados da lista são convertidos para json, separando seus registros, e é realizado uma tentativa de armazenamento no bucket que retorna sucesso caso os dados forem salvos.

```py
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
```

- Os dados são salvos no arquivo *detalhes_filmes.json* contendo **29 registros**, com a mesma estrutura,  e *9.2KB*. 

- Caso queira conferir a estrutura: - [Arquivo Json](/etapa-2-lambda/data/detalhes_filmes.json)

- [Função Completa](/PROJETO%20II/etapa-2-lambda/lambda-function.py)

- [Arquivos de Testes Locais](/PROJETO%20II/etapa-2-lambda/testes/)

- [Layers](/PROJETO%20II/etapa-2-lambda/layers/Dockerfile) *Foram ignorados por razões de armazenamento*