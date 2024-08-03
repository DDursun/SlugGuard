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

            



            total_cases = len(data)
            # Finding number of total slugging cases in a given time frame
            slugging_cases = (data["predicted_class"] == 3).sum()
            slugging_to_normal_ratio = (slugging_cases / total_cases)*100
            print(slugging_to_normal_ratio)
        
            # Activating notification system if 2% of timeframe indicates slugging
            if slugging_to_normal_ratio > 2:
                send_message(f"Completed processing for well: {well}, There are {slugging_cases} data points indicating slugging in {well}")
                plot_well(data, well, window_size, threshold)
                send_image(well)

            else:
                send_message(f"Completed processing for well: {well}")
                send_message(f"No slugging detected in well {well}")

            # Send latest operating conditions
            last_temp = round(data["T_TPT"].iloc[-1],2)
            last_pressure = round(data["P-TPT-psi"].iloc[-1],2)
            send_message(f"Current temperature is {last_temp}F, pressure (psi) is {last_pressure}")

            #Send special message if slugging continues in any last 20 points
            slugging_condition = (data["predicted_class"].iloc[-20:] == 3).any()
            if slugging_condition:
                send_message(f"{well} is still slugging, immediate action is needed!")

            # Adding all wells into one dataframe for updating database / labels
            finaldbdf = pd.concat([finaldbdf, data], ignore_index=True)



        """"
        all_cases = len(finaldbdf)
        correct_cases = (finaldbdf["predicted_class"] == finaldbdf["class"]).sum()
        accuracy = correct_cases / all_cases * 100
        print(format(accuracy, ".2f"))

        """


    finaldbdf = finaldbdf.drop(columns=['RowNum', 'fluctuation','choke_change'])
    print("Run completed at: ", datetime.datetime.now())
    #write_todb(finaldbdf)

if __name__ == "__main__":
    main()

    while True:
        run_pending()
        time.sleep(1)