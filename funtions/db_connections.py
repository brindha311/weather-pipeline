import pandas as pd
from sqlalchemy import create_engine
# pymysql lib is also used to connect to mysql db and to the sql query
import pymysql

# connect to mysql db
def database_connection():
    db_username = 'admin'
    db_password = 'admin'
    db_host = '127.0.0.1'
    db_name = 'weather'
    engine = create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_name}')
    return engine


def database_connection_pymysql():
    # Establishing a connection to the MySQL server
    connection = pymysql.connect(
        host='127.0.0.1',
        user="admin",
        password="admin",
        database="nkw",
        cursorclass=pymysql.cursors.DictCursor
    )
    # Creating a cursor object to execute queries
    cursor = connection.cursor()
    return cursor
