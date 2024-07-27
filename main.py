from data_access.db_connection import fetch_data
from alert.send_alert import send_message
from config.config import config
from utils import *
import pandas as pd
import matplotlib.pyplot as plt

window_size = 20  # Define the window size (number of data points)
threshold = 40  # Define the threshold for fluctuations


def process_well_data(well):

    #   Reading last 10 days data for profile creation
    query = f"""SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY timestamp DESC) as RowNum
                    FROM allwells
                    WHERE well = '{well}'
                ) sub
                WHERE RowNum <= 14400
                ORDER BY RowNum"""

    data = fetch_data(query)
    if data is not None:
        # Process the data for the well
        data["P-TPT-psi"] = data["P_TPT"]*0.000145037738
        #send_message(f"Processing data for well: {well}")

    
    return(data)

def main():
    wells = get_distinct_wells()

    finaldbdf = pd.DataFrame()
    
    if wells is not None:
        for well in wells:
            data = process_well_data(well)
            #send_message(f"Completed processing for well: {well}")
            data = detect_and_label_fluctuations(data,"P-TPT-psi")
            data = detect_and_label_choke_changes(data)
            data = apply_rules(data)
            finaldbdf = finaldbdf.append(data)
        
        total_cases = len(finaldbdf)
        correct_cases = (finaldbdf["predicted_class"] == finaldbdf["class"]).sum()
        accuracy = correct_cases / total_cases * 100
        print(accuracy)


    finaldbdf = finaldbdf.drop(columns=['RowNum', 'fluctuation','choke_change'])
    print("Ready to write to db")
    #write_todb(finaldbdf)


if __name__ == "__main__":
    main()
