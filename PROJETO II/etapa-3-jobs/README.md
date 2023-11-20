# Informações sobre Jobs

## Job arquivos CSV

**No Glue, comecei criando um job para os dados dos arquivos CSV, e dropando todas as tabelas que não utilizei**

**INPUT_PATH** - s3://projeto-2/data-lake-projeto-2/Raw/input-local/2023/11/14/filmes.csv

**TARGET_PATH** - s3://projeto-2/data-lake-projeto-2/Trusted/CSV/

- [Job Completo](/etapa-3-jobs/jobs/trusted-job-csv.py)

Após ter os dados necessários em um dataframe, alterei o esquema de todas as colunas

```py
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
```
Obs: A coluna anoLancamento foi setada como *int* por se tratar somente do ano do lançamento, caso fosse um *date* o formato seria *yyyy-mm-dd*

**As colunas foram renomeadas, foi feito uma filtragem com o nome do artista para realizar a análise, e enviadas para o *TARGET_PATH* com uma repartição**

```py
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
```

**Os dados foram passados para o Catalog utilizando Crawlers e enviados para o database *tl_trusted_zone***

**Lembrando que o Job Completo e detalhado se encontra aqui**

- [Job Completo](/etapa-3-jobs/jobs/trusted-job-csv.py)

## Job arquivos JSON

**INPUT_PATH** - s3://projeto-2/data-lake-projeto-2/Raw/TMDB/JSON/2023/11/16/detalhes_filmes.json

**TARGET_PATH** - s3://projeto-2/data-lake-projeto-2/Trusted/TMBD/

- [Job Completo](/PROJETO%20II/etapa-3-jobs/jobs/trusted-job-json.py)

**Ao importar os dados, reparei que na criação da coluna *receita* o Glue cria uma *struct* e passa dois tipos de dados, *int* e *long***

![Colunas logs](/img/colunas-json.png)

Isso ocorreu pois apenas um dado da coluna receita passava de 9 algarismos, e era considerado como *bigint* ou *long*.

Para normalizar esses dados, selecionei os dados int e long em dataframes diferentes, trazendo o imdb_id e filtrei somente os dados que não são nulos e setei os dados do formato *int* como *long*.

```py
#Seleciona as estruturas da coluna receita, retira as nulas, junta as tabelas 
df_receita_long = df.select(col('receita.long').alias('receita_long'), 'imdb_id')
df_receita_long = df_receita_long.filter(col('receita_long').isNotNull())

df_receita_int = df.select(col('receita.int').alias('receita_int'), 'imdb_id')
df_receita_int = df_receita_int.filter(col('receita_int').isNotNull())
df_receita_int = df_receita_int.withColumn('receita_int', col('receita_int').cast('long'))

#retira a coluna receita antiga 
df = df.drop('receita')
```

Após a filtragem, realizei um join com os dataframes pelo *imdb_id* e os dados foram combinados em uma coluna *receita* utilizando a função *coalesce*, fiz um drop nas colunas antigas e um join da nova coluna receita com todos os dados long, no dataframe DF. 

```py
# Junção dos DataFrames e cria a nova coluna 'receita' com os dados formatados em long
df_receita = df_receita_int.join(df_receita_long, 'imdb_id', 'outer')
df_receita = df_receita.withColumn('receita', coalesce(col('receita_long'), col('receita_int')))
df_receita = df_receita.drop('receita_int').drop('receita_long')

# Junta receita com a tabela antiga
df = df_receita.join(df, 'imdb_id', 'outer')
```

**Com a modelagem relacional das tabelas definidas, já comecei as alterações necessárias para as novas tabelas com dados atômicos, que só ficariam prontas posteriormente na etapa *Refined***

Criei um novo dataframe *df_genero* selecionando os dados da coluna genero, eliminando os dados duplicados e criando id's para cada gênero. 

Outro dataframe *df_filme_genero* foi criado selecionando o *imdb_id* dos filmes, e trazendo os gêneros.

Ainda no *df_filme_genero*, foi realizado um join com o *df_genero* utilizando *right_outer* levando os dados do dataframe *df_genero* para o *df_filme_genero* e dropando a coluna genero, afim de manter na tabela *df_filme_genero* somente *imdb_id* e *id_genero*.

Após essas alterações, foi criado um id único para o dataframe *df_filme_genero* e ordenado pelo mesmo, deixando a tabela completamente pronta para acessar todos os gêneros de um determinado filme.

```py
# Tabela genero
df_genero = df.select(explode("generos").alias("genero")).distinct().withColumn("id_genero", monotonically_increasing_id())

# Tabela filme_genero
df_filme_genero = df.select('imdb_id', explode('generos').alias('genero'))
df_filme_genero = df_filme_genero.join(df_genero, 'genero', 'right_outer').drop(col('genero')).withColumn("id_filme_genero", monotonically_increasing_id()).orderBy('id_filme_genero')
```
**Com a coluna produtoras o processo foi básicamente igual**

Criei um novo dataframe *df_produtora* selecionando os dados da coluna produtora, eliminando os dados duplicados e criando id's para cada produtora. 

Outro dataframe *df_filme_produtora* foi criado selecionando o *imdb_id* dos filmes, e trazendo as produtoras.

Ainda no *df_filme_produtora*, foi realizado um join com o *df_produtora* utilizando *right_outer* levando os dados do dataframe *df_produtora* para o *df_filme_produtora* e dropando a coluna produtora, afim de manter na tabela *df_filme_produtora* somente *imdb_id* e *df_produtora*.

Após essas alterações, foi criado um id único para o dataframe *df_filme_produtora* e ordenado pelo mesmo, deixando a tabela completamente pronta para acessar todas as produtoras de um determinado filme.

```py
# Tabela produtora
df_produtora = df.select(explode("produtoras").alias("produtora")).distinct().withColumn('id_produtora', monotonically_increasing_id())

# Tabela filme_produtora
df_filme_produtora = df.select('imdb_id', explode('produtoras').alias('produtora'))
df_filme_produtora = df_filme_produtora.join(df_produtora, 'produtora', 'right_outer').drop(col('produtora')).withColumn("id_filme_produtora", monotonically_increasing_id()).orderBy('id_filme_produtora')
```

**Com a normalização dos dados, foram alterados novamente para DynamicFrame e enviados para o *TARGET_PATH* com apenas uma repartição, cada qual em suas pastas para criação das tabelas com Crawler**

```py
df = df.repartition(1)
df = DynamicFrame.fromDF(df, glueContext,"dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame= df, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/19/dados_filme"},
    format = 'parquet'
    )
```
**Lembrando que o Job Completo e detalhado se encontra aqui**

- [Job Completo](/etapa-3-jobs/jobs/trusted-job-csv.py)

**Os dados foram passados para o Catalog utilizando Crawlers e enviados para o database *tl_trusted_zone***

![Crawlers](/img/crawlers.png)

![Banco de Dados](/img/databases.png)

![Tabelas](/img/tables.png)