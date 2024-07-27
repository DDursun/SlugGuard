from data_access.db_connection import fetch_data
from expert import *

def get_distinct_wells():
    query = 'SELECT DISTINCT well FROM [sensor_data].[dbo].[allwells]'
    wells_data = fetch_data(query)
    wells = wells_data['well'].tolist()
    return wells
    
def detect_and_label_fluctuations(data, column, window_size=30, threshold=35):
    # Calculate the exponential rolling mean and standard deviation
    ewm_mean = data[column].ewm(span=window_size, adjust=False).mean()
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










