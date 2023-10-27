# Interação com MongoDB

mongosh
>start mongo

pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1
>start pyspark com mongo

posts = spark.read.format("mongo").option("uri","mongodb://localhost/posts.post").load()
>rodar documentos no data frame

posts.write.format("mongo").option("uri","mongodb://localhost/posts2.post").save()
>gravar no mongo

mongoimport --db 'db-name' --collection 'collection-name' --legacy --file 'file-location'
>importar arquivo pro mongo