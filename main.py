from data_access.db_connection import fetch_data
from alert.send_alert import *
from config.config import config
from utils import *
import pandas as pd
from schedule import every, repeat, run_pending
import datetime
import time

window_size=30  # Define the window size (number of data points)
threshold=35  # Define the threshold for fluctuations


def process_well_data(well):

    #   Reading last 10 days data for profile creation
    query = f"""SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER (ORDER BY timestamp ASC) as RowNum
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

@repeat(every(10).minutes)
def main():
    wells = get_distinct_wells()

    finaldbdf = pd.DataFrame()
    
    if wells is not None:
        for well in wells:
            data = process_well_data(well)
           
            data = detect_and_label_fluctuations(data,"P-TPT-psi")
            data = detect_and_label_choke_changes(data)
            data = apply_rules(data)

            case = None 

            plot_well(data, well, window_size, threshold)
            send_image(well)
            send_message(f"Completed processing for well: {well}")

            
            """
            send_message(f"Completed processing for well: {well}")
            if case is not None:
                #send_plot()
                pass
            else:
                send_message(f"No slugging detected in well {well}")

            """    


            finaldbdf = pd.concat([finaldbdf, data], ignore_index=True)



        total_cases = len(finaldbdf)
        correct_cases = (finaldbdf["predicted_class"] == finaldbdf["class"]).sum()
        accuracy = correct_cases / total_cases * 100
        print(format(accuracy, ".2f"))


    finaldbdf = finaldbdf.drop(columns=['RowNum', 'fluctuation','choke_change'])
    print("Run completed at: ", datetime.datetime.now())
    #write_todb(finaldbdf)

if __name__ == "__main__":
    main()

    while True:
        run_pending()
        time.sleep(1)