from data_access.db_connection import fetch_data
import matplotlib.pyplot as plt
from expert import *

def get_distinct_wells():
    query = 'SELECT DISTINCT well FROM [sensor_data].[dbo].[allwells]'
    wells_data = fetch_data(query)
    wells = wells_data['well'].tolist()
    return wells
    
def detect_and_label_fluctuations(data, column, window_size=30, threshold=35):
    # Calculate the exponential rolling mean and standard deviation
    ewm_mean = data[column].ewm(span=window_size, adjust=True).mean()
    # Calculate the difference from the exponential rolling mean
    fluctuation = (data[column] - ewm_mean).abs()
    # Detect where the fluctuation exceeds the threshold
    data['fluctuation'] = (fluctuation > threshold).astype(int)
    return data

def detect_and_label_choke_changes(data, window=10):

    # Create the 'choke_change' column with default value 0
    data['choke_change'] = 0
    
    for i in range(len(data)):
        # Define the range for lookback
        start_index = max(0, i - window)
        current_value = data.at[i, 'choke']
        
        # Get the 'choke' values in the lookback range
        previous_values = data['choke'].iloc[start_index:i]
        
        # Check if there's a different value in the previous rows
        if (previous_values != current_value).any():
            data.at[i, 'choke_change'] = 1
    
    return data


    
def apply_rules(df):
    engine = SluggingExpertSystem()
    results = []
    for _, row in df.iterrows():
        engine.reset()
        engine.declare(Fact(fluctuation=row['fluctuation']))
        engine.declare(Fact(choke_change=row['choke_change']))
        engine.run()
        result_fact = next((fact for fact in engine.facts.values() if 'result' in fact), None)
        result = result_fact['result'] if result_fact else None
        results.append(result)
    
    df['predicted_class'] = results
    return df


def write_todb(df):
        df.to_sql('allwells', if_exists='replace', index=False)


def plot_well(data, well, window_size, threshold):

    # Filter to only periods where fluctuation is True
    fluctuation_periods = data[data['fluctuation'].astype(bool)]

    # Plot the results
    plt.figure(figsize=(14, 10))

    # Plot the original pressure data
    plt.plot(data['timestamp'], data['P-TPT-psi'], label='Pressure', color='blue')

    # Plot the EMA
    plt.plot(data['timestamp'], data['P-TPT-psi'].ewm(span=window_size, adjust=False).mean(), label='Exponential Moving Average (EMA)', color='green', marker='o', markersize=1)
    

    # Highlight the detected fluctuation periods
    if not fluctuation_periods.empty:
        start_idx = None
        for i in range(len(fluctuation_periods)):
            if start_idx is None:
                start_idx = fluctuation_periods.index[i]
            if i == len(fluctuation_periods) - 1 or fluctuation_periods.index[i+1] != fluctuation_periods.index[i] + 1:
                plt.axvspan(data['timestamp'][start_idx], data['timestamp'][fluctuation_periods.index[i]], color='red', alpha=0.3)
                start_idx = None

    plt.xlabel('Timestamp')
    plt.ylabel('Pressure')
    plt.title(f'{well} - Pressure Trend with Detected Fluctuations > {threshold} psi')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{well}.jpg")




































