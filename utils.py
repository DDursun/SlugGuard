from data_access.db_connection import fetch_data

def get_distinct_wells(connection_string):
    query = 'SELECT DISTINCT well FROM [sensor_data].[dbo].[allwells]'
    wells_data = fetch_data(connection_string, query)
    wells = wells_data['well'].tolist()
    return wells

def detect_fluctuations(series, window_size, threshold):
    # Calculate the difference between the current value and the value at the start of the window
    fluctuation = series.diff(window_size).abs()
    return fluctuation > threshold
