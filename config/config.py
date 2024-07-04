class Config:
    # Database Configuration
    
    CONNECTION_STRING = (
    r'mssql+pyodbc://DESKTOP-PIOPBBU\SDD/sensor_data?'
    r'driver=ODBC+Driver+17+for+SQL+Server&'
    r'Trusted_Connection=yes'
)


config = Config()
