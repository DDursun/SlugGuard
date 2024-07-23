from data_access.db_connection import fetch_data
from alert.send_alert import send_message
from config.config import config
from utils import *

window_size = 20  # Define the window size (number of data points)
threshold = 45  # Define the threshold for fluctuations


def process_well_data(connection_string, well):

    #   Reading last 10 days data for profile creation
    query = f"SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY timestamp DESC) as RowNum
                    FROM allwells
                    WHERE well = '{well}'
                ) sub
                WHERE RowNum <= 14400
                ORDER BY RowNum"

    data = fetch_data(connection_string, query)
    if data is not None:
        # Process the data for the well
        #send_message(f"Processing data for well: {well}")
        pass
    
    return(data) 

def label_fluctutation(data):
    data['fluctuation'] = detect_fluctuations(data['P-TPT-psi'], window_size, threshold)
    return data

def main():
    connection_string = config.CONNECTION_STRING
    wells = get_distinct_wells(connection_string)
    
    if wells is not None:
        for well in wells:
            process_well_data(connection_string, well)
            #send_message(f"Completed processing for well: {well}")

if __name__ == "__main__":
    main()
