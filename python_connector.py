import snowflake.connector
import os
import base64
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import pandas as pd

def read_data(connection):
    try:
        query='''select * from kpdb.public.employee'''
        data = pd.read_sql(query, connection)
        print(type(data))
        print("####### Reading Completed #############")
        return data
    finally:
        connection.close()

def transform_data(data):
    try:
        data['name']=data['name'].str.upper()
        print(type(data))
        print(data)
        return data
    finally:
        connection.close()

def write_data(data):
    try:
        data.to_sql('kp_python', engine, if_exists='replace', index=False, index_label=None)
    finally:
        connection.close()


        
if __name__ == "__main__":
    # Gets the version
    url = URL(
        user='***********',
        password=base64.urlsafe_b64decode('*********************'.encode('UTF-8')).decode('ascii'),
        account='**********************',
        database = 'kpdb',
        schema = 'public',
    )
    engine = create_engine(url)
    connection = engine.connect()
    input_data = read_data(connection)
    final_data = transform_data(input_data)
    write_data(final_data)
    
