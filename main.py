from data_access.db_connection import fetch_data
from alert.send_alert import send_message
from config.config import config

def get_distinct_wells(connection_string):
    query = 'SELECT DISTINCT well FROM [sensor_data].[dbo].[allwells]'
    wells_data = fetch_data(connection_string, query)
    wells = wells_data['well'].tolist()
    return wells

def process_well_data(connection_string, well):
    query = f"SELECT * FROM [sensor_data].[dbo].[allwells] WHERE well = '{well}'"
    data = fetch_data(connection_string, query)
    if data is not None:
        # Process the data for the well
        send_message(f"Processing data for well: {well}")

def main():
    connection_string = config.CONNECTION_STRING
    wells = get_distinct_wells(connection_string)
    print(wells)
    
    if wells is not None:
        for well in wells:
            process_well_data(connection_string, well)
            send_message(f"Completed processing for well: {well}")

if __name__ == "__main__":
    main()
