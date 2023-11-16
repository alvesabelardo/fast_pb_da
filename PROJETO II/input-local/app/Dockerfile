FROM python:3

WORKDIR /app

COPY index.py /app

RUN pip3 install boto3

VOLUME /data

COPY dados/movies.csv /data/movies.csv
COPY dados/series.csv /data/series.csv

CMD ["python", "index.py"]