from data_access.db_connection import fetch_data
from alert.send_alert import send_message
from config.config import config

def main():
    # Data Ingestion
    data = fetch_data(config.CONNECTION_STRING, 'SELECT * FROM [sensor_data].[dbo].[WELL-00001]')
    
    if data is not None:
        send_message("Hi, I can see the data now!")
    

if __name__ == "__main__":
    main()
