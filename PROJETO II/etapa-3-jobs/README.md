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

**INPUT_PATH** - s3://projeto-2/data-lake-projeto-2/Raw/TMDB/JSON/2023/11/23/detalhes_filmes.json

**TARGET_PATH** - s3://projeto-2/data-lake-projeto-2/Trusted/TMBD/

- [Job Completo](/PROJETO%20II/etapa-3-jobs/jobs/trusted-job-json.py)

**Na importação dos arquivos JSON, tive alguns problemas com o *Create Dynamic Frame*, que começou a me causar problemas com dados sendo apagados e armazenamento sendo economizado, criando Structs dentro de colunas.**

Apos um tempo de pesquisa e testes, consegui importar os dados utilizando o PySpark, setando o schema anteriormente

```py
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
```

**Com a modelagem relacional das tabelas definidas, já comecei as alterações necessárias para as novas tabelas com dados atômicos, que só ficariam prontas posteriormente na etapa *Refined***

Criei um novo dataframe *df_genero* selecionando os dados da coluna genero, eliminando os dados duplicados e criando id's para cada gênero. 

```py
df_genero = df.select(explode("generos").alias("genero")).distinct()
df_genero = df_genero.withColumn("id_genero", row_number().over(Window.orderBy("genero")))
```

Outro dataframe *df_filme_genero* foi criado selecionando o *imdb_id* dos filmes, e trazendo os gêneros.

Ainda no *df_filme_genero*, foi realizado um join com o *df_genero* utilizando *right_outer* levando os dados do dataframe *df_genero* para o *df_filme_genero* e dropando a coluna genero, afim de manter na tabela *df_filme_genero* somente *imdb_id* e *id_genero*.

Após essas alterações, foi criado um id único para o dataframe *df_filme_genero* e ordenado pelo mesmo, deixando a tabela completamente pronta para acessar todos os gêneros de um determinado filme.

```py
df_filme_genero = df.select('imdb_id', explode('generos').alias('genero'))
df_filme_genero = df_filme_genero.join(df_genero, 'genero', 'right_outer')
df_filme_genero = df_filme_genero.withColumn('id_filme_genero', row_number().over(Window.orderBy('genero'))).drop(col('genero'))
```

**Com a coluna produtoras o processo foi básicamente igual**

Criei um novo dataframe *df_produtora* selecionando os dados da coluna produtora, eliminando os dados duplicados e criando id's para cada produtora. 

```py
df_produtora = df.select(explode("produtoras").alias("produtora")).distinct()
df_produtora = df_produtora.withColumn("id_produtora", row_number().over(Window.orderBy("produtora")))
```

Outro dataframe *df_filme_produtora* foi criado selecionando o *imdb_id* dos filmes, e trazendo as produtoras.

Ainda no *df_filme_produtora*, foi realizado um join com o *df_produtora* utilizando *right_outer* levando os dados do dataframe *df_produtora* para o *df_filme_produtora* e dropando a coluna produtora, afim de manter na tabela *df_filme_produtora* somente *imdb_id* e *df_produtora*.

```py
df_filme_produtora = df.select('imdb_id', explode('produtoras').alias('produtora'))
df_filme_produtora = df_filme_produtora.join(df_produtora, 'produtora', 'right_outer')
df_filme_produtora = df_filme_produtora.withColumn('id_filme_produtora', row_number().over(Window.orderBy('produtora'))).drop(col('produtora'))
```

Após essas alterações, foi criado um id único para o dataframe *df_filme_produtora* e ordenado pelo mesmo, deixando a tabela completamente pronta para acessar todas as produtoras de um determinado filme.

Dropei colunas do dataframe principal que já foram alocadas em outras tabelas, e optei por não usar a coluna *lingua_original* já que a lingua original de todos os filmes são em Inglês

```py
df = df.drop(col('generos'))
df = df.drop(col('produtoras'))
df = df.drop(col('lingua_original'))
```


**Com a normalização dos dados, foram alterados novamente para DynamicFrame e enviados para o *TARGET_PATH* com apenas uma repartição, cada qual em suas pastas para criação das tabelas com Crawler**

```py
df = DynamicFrame.fromDF(df, glueContext,"dynamic_frame")
glueContext.write_dynamic_frame.from_options(
    frame= df, 
    connection_type = 's3',
    connection_options = {"path": f"{target_path}/2023/11/23/dados_filme"},
    format = 'parquet'
    )
```

**Lembrando que o Job Completo e detalhado se encontra aqui**

- [Job Completo](/etapa-3-jobs/jobs/trusted-job-csv.py)

**Os dados foram passados para o Catalog utilizando Crawlers e enviados para o database *tl_trusted_zone***

![Crawlers](/img/crawlers.png)

![Banco de Dados](/img/databases.png)

![Tabelas Trusted Zone](/img/tables.png.png)

## Job Refined

**INPUT_PATH** - s3://projeto-2/data-lake-projeto-2/Trusted/

**TARGET_PATH** - s3://projeto-2/data-lake-projeto-2/Refined/

-[Job Completo](/PROJETO%20II/etapa-3-jobs/jobs/refined-job.py)

**Com a importação dos arquivos para o Catalog utilizando Crawler, na camada Refined, as tabelas foram lidas diretamentee do Catalog.**


```py
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
```

**Após a leitura, já alterei-as para DataFrame marcando com um *#Done* as que já estavam prontas** 

E ordenei cada tabela pronta pelo seu ID único

```py
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
```

**Realizei um join dos dados complementares que vieram do arquivo *movies.csv* com os dados introduzidos da API TMDB**

Dropei todas as colunas que já estavam localizadas em outras tabelas.

Renomei a coluna *nomeArtista* para *Ator* já que se trata somende de Leonardo DiCaprio.

```py
all_data = df_dados_filme.join(df_dados_complementares, 'imdb_id', 'outer') #Todos dados necessários para as outras tabelas

filme = all_data.drop('receita').drop('popularidade').drop('orcamento').drop('media_votos').drop('quantidade_votos') 
filme = filme.withColumnRenamed('nomeArtista', 'ator') #Done
```

**Aqui comecei a trabalhar na tabela *filme_lucro, puxando os dados do data frame *all_data* e dropando as tabelas para mante-lá como planeijei na modelagem relacional do banco.**

Gerei uma nova coluna de *lucro*, diminuindo a receita pelo orçamento, e uma nova coluna de *percent_lucro* onde é calculada a porcentagem de lucro. 

Novamente doi criada um id único para essa coluna, utilizando *row_number* e ordenando pelo *imdb_id*

É importante destacar que a API TMDB não tinha acesso a todos os dados dessa tabela, como *orçamento* e *receita* de todos os filmes, que poderam retornar alguns valores igual a zero, por falta de informações.

```py
filme_lucro = all_data.drop('popularidade').drop('titulo').drop('lancamento').drop('duracao_minutos').drop('media_votos').drop('quantidade_votos').drop('nomeArtista')
filme_lucro = filme_lucro.withColumn("lucro", (col("receita") - col("orcamento")).cast(IntegerType()))
filme_lucro = filme_lucro.withColumn("percent_lucro", (col("lucro") / col("orcamento")) * 100).withColumn("percent_lucro", col("percent_lucro").cast(FloatType()))
filme_lucro = filme_lucro.withColumn('id_filme_lucro', row_number().over(Window.orderBy('imdb_id'))) #Done
```

**Na coluna *filme_score*, onde contém informações sobre *popularidade, votos e média de votos*, também foram dropadas as colunas que já estão mapeadas em outras tabelas, e criado um id de identificação único**

```py
filme_score = all_data.drop('receita').drop('orcamento').drop('titulo').drop('lancamento').drop('duracao_minutos').drop('nomeartista')
filme_score = filme_score.withColumn('id_filme_score', row_number().over(Window.orderBy('imdb_id'))) #Done
```

**Com todas as 7 colunas prontas seguindo o *modelo relacional*, importei as para a camada Refined, e depois utilizei um Crawler para criar as tabelas**

- [Modelo Relacional](/img/modelagem-tabelas.jpg)

```py
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
```

Exportando tabelas com Crawler

![Exportando para Catalog com Crawler](/img/crawler-refined.png)

Tabelas Criadas no banco de dados *tl_refined_zone*

![Tabelas no banco de dados tl_refined_zone](/img/tl_refined_zone_db.png)

![Job completo](/PROJETO%20II/etapa-3-jobs/jobs/refined-job.py)