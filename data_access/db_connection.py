import pandas as pd
from sqlalchemy import create_engine, text
from config.config import config


connection_string = config.CONNECTION_STRING
dbengine = create_engine(connection_string)

def fetch_data(query, dbengine=dbengine):
    with dbengine.connect() as connection:
        data = pd.read_sql(text(query), connection)
    return data
