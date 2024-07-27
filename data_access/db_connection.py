import pandas as pd
from sqlalchemy import create_engine, text

def fetch_data(connection_string, query, engine):
    with engine.connect() as connection:
        data = pd.read_sql(text(query), connection)
    return data
