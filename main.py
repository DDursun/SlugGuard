from data_access.db_connection import fetch_data

CONNECTION_STRING = (
    r'mssql+pyodbc://DESKTOP-PIOPBBU\SDD/sensor_data?'
    r'driver=ODBC+Driver+17+for+SQL+Server&'
    r'Trusted_Connection=yes'
)

QUERY = 'SELECT * FROM [sensor_data].[dbo].[WELL-00001]'


def main():
    # Data Ingestion
    data = fetch_data(CONNECTION_STRING, QUERY)
    print("Your data is ready!")
    print(data.head())
   
   
if __name__ == "__main__":
    main()
