import pandas as pd
import matplotlib.pyplot as plt
import json
from os import path

# Edit here START ------------
sources = [('3456', '70091'), ('46964', '12485'), ('77532', '50224'), ('2636415', '12053'), ('87107442643', '63593')]
# Edit here STOP -------------

for source in sources:
    internal_id = source[0]
    bike_number = source[1]
    all_data = []
    api_request_page_count = 1
    meas_json_file = f'meas_json_results/{internal_id}.json'

    try:
        with open(meas_json_file, 'r') as file:
            all_data = json.load(file)
    except FileNotFoundError as e:
        print('Error accessing measurements file:', e, 'exiting...')
        exit()

    # Flatten the nested dictionaries within the 'payload' key
    flattened_data = pd.json_normalize([item['payload'] for item in all_data])

    # Add the 'time' column to the flattened DataFrame
    flattened_data['time'] = [item['time'] for item in all_data]

    # Convert 'time' column to datetime
    flattened_data['time'] = pd.to_datetime(flattened_data['time'])
    flattened_data.set_index('time', inplace=True)
    flattened_data.sort_index(inplace=True)

    print(internal_id)
    print(bike_number)
    print(flattened_data.head(3), flattened_data.tail(3), sep = '\n')
    print('--------------------------------------------------------------------------------------------------------------------------------')
    flattened_data.to_csv(f'meas_csv_results/{bike_number}.csv')

    