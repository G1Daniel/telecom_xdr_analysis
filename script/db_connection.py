import pandas.io.sql as sqlio
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import pandas as pd

def db_connection_sqlalchemy():
    # database connection using sqlalchemy
    database_name='telecom'
    table_name='xdr_data'
    connection_params={
        "host":"localhost",
        "user":"postgres",
        "password":"admin",
        "port":"5432",
        "database":database_name
    }
    engine = create_engine(f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}")
    return engine

   