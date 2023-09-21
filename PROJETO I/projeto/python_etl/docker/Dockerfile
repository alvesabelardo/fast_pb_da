FROM python:3

WORKDIR /python_etl

COPY . .  

RUN pip3 install pandas
RUN pip3 install psycopg2
RUN pip3 install pymongo

CMD [ "python", "./etl.py" ]