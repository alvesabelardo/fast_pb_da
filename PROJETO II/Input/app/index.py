
import boto3
import os 

filmes = os.path.join("/data", "movies.csv")
series = os.path.join("/data", "series.csv")

client = boto3.client('s3',
    aws_access_key_id='ASIAT3YCQ4HZE23E6WHN',
    aws_secret_access_key='MYHkaJj2NbHuQX5p1/0cdYQsXr9DsIEfVu3wVvWG',
    aws_session_token="IQoJb3JpZ2luX2VjEBYaCXVzLWVhc3QtMSJIMEYCIQCZm/xFu94E1qbVfv0OQJFe4N4eGAjc73++1D/pmhlysQIhANPW98feKNy/ZXVddqS/YZoziDWKm4mG4kwN2AdVlC/SKqEDCF8QABoMMjY1NzU2NDAyMTYyIgzRrFWyYfJHDdOl2GYq/gIsGDscPffSJAU/9QDg2bHDj3Anf3MLMnd0N31NEeDk6JNLpLF6eTsTQBa1Ng5bGgeLO143JutCDgo42a+hPtQzp2HJwRhKycwgCgD5/myXK7jTUxfHGmitmrv+5QFJzGpL7oIVptLqqqDO+0oKWTghFI+ECUyqh2zf6pQYD5KMqZzk3MaNi5jUW4LDtRlTkr+Lrow7PkkhnhRltL1hDkUknpy9zPYUvgqsG3xdsZSBSaG7wowfMWFbahqXZc7jsozg3eXIfH1SLgbb4gArK79XDcrpz0j/69G5AKJWLRaFAuur3qPgCtR/JV1HZ3htYHDcBkOvuqX8Up4aGP9eR6Y8P/S4gJihdL4LqeI9llBHqfcHqtmwdlKov5jXiMEt+XzajRXAZN+wbrvXtxxdaZFkFwXPk/gAHdhlaLMM1V7EmOoEqHrp/6DncxxRIG4jNSdWaTLxAzBCoTCZv1yxovfJdQc2pN2Swc178/Bh+xgXPF/Q5+ozIP9HnQvgjQseMPLxzaoGOqUBbSGKmUo/70FU/1l8eWMJZAAjZkrPSR4AvJXDzPYOHdrliMd5qVYAWCHzDqQOJYjnvG+1FsewVLgsTCsHdR+0/qG/Iigr8Fd6KK+atAgnesX2eAg0Z2XjAhcfeoEIGtuJRTrNxoiR74IYKqcjRD37c28aR3pTIY64fDjwI8uOjRuo6BpPK0rjnhEnipjKRJw6zC2FXhsvB+wuxUvrTSh0hVyTqq8p"
)
bucket_name = "projeto-2"
object_key_filmes = 'data-lake-projeto-2/Raw/input-local/2023/11/14/filmes.csv'
object_key_series = 'data-lake-projeto-2/Raw/input-local/2023/11/14/series.csv'

try:
    client.upload_file(filmes, bucket_name, object_key_filmes)
    client.upload_file(series, bucket_name, object_key_series)
    print(f"Upload bem-sucedido: {filmes} para s3://{bucket_name}/{object_key_filmes}")
    print(f"Upload bem-sucedido: {series} para s3://{bucket_name}/{object_key_series}")
except Exception as e:
    print(f"Erro durante o upload: {e}")
