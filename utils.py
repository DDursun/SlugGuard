from data_access.db_connection import fetch_data

def get_distinct_wells(connection_string):
    query = 'SELECT DISTINCT well FROM [sensor_data].[dbo].[allwells]'
    wells_data = fetch_data(connection_string, query)
    wells = wells_data['well'].tolist()
    return wells
    
def detect_and_label_fluctuations(data, column, window_size, threshold):
    # Calculate the exponential rolling mean and standard deviation
    ewm_mean = data[column].ewm(span=window_size, adjust=False).mean()
    # Calculate the difference from the exponential rolling mean
    fluctuation = (data[column] - ewm_mean).abs()
    # Detect where the fluctuation exceeds the threshold
    data['fluctuation'] = fluctuation > threshold
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


    













