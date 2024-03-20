import pandas as pd
import matplotlib.pyplot as plt
import json
from os import path


devices_list = [63589, 70091, 63593, 12053, 12485, 12483, 70079, 17018, 14912, 
              17020, 50221, 96168, 98681, 14787, 97405, 12108, 72748, 50224, 
              70080, 96199, 14913, 17019, 70089]
source_ids_list = [47107417697, 3456, 87107442643, 2636415, 46964, 47674, 1068248, 2002381, 26637896,
                   439804, 1422168, 3420, 2483, 34695733, 49237155, 69305, 60107407843, 77532,
                   3482, 3241, 29139236, 434557, 2508]

# No data from 01 Jan onwards: 12483 - 96168
# Online but danger: 63589, 97405, 14913
# No data: 72748 (Did not connect after installation)
# No data on c8y: 12483, 70079, 17018, 14912, 17020, 96168

# Edit here START ------------
sources_to_make = [-1] # list(range(len(devices_list)))
# Edit here STOP -------------

sources = [(source_ids_list[-1], devices_list[-1])]

for source in sources:
    internal_id = source[0]
    bike_number = source[1]
    all_data = []
    api_request_page_count = 1
    meas_json_file = f'./c8y/meas_json_results/{internal_id}.json'

    try:
        with open(meas_json_file, 'r') as file:
            all_data = json.load(file)
    except FileNotFoundError as e:
        print('Error accessing measurements file:', e, 'moving on...')
        continue

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
    flattened_data.to_csv(f'./c8y/meas_csv_results/{bike_number}.csv')

    