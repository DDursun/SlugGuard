import pandas as pd
from sqlalchemy import create_engine, text

def fetch_data(connection_string, query):
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        data = pd.read_sql(text(query), connection)
    return data
