import pandas as pd
import json
from ignore_constants import *

# NO DATA FOR THESE (as of 2024-05-13): 63593, 14787, 14912, 70079, 72748, 96168, 11865
# DATA WAY FAR BACK (as of 2024-05-15): 50221, 50224, 12483, 98681, 12287

try:
    # Edit here START ------------
    devices_of_interest = [10622]

    sources_to_make = [devices_list.index(x) for x in devices_of_interest]

except ValueError as e:
    print('[Error]: Device number not found!', end=' ')
    print(e)
    exit()

sources = [(source_ids_list[i], devices_list[i]) for i in sources_to_make]
count = 1
num_of_sources = len(sources)

for source in sources:
    internal_id = source[0]
    device_number = source[1]
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

    # Add the 'device_time' column to the flattened DataFrame
    flattened_data['device_time'] = [item['time'] for item in all_data]
    flattened_data['server_time'] = [item['payload']['ServerTime']['value'] for item in all_data]

    # Convert 'device_time' column to datetime
    flattened_data['device_time'] = pd.to_datetime(flattened_data['device_time'])
    flattened_data['server_time'] = pd.to_datetime(flattened_data['server_time'], unit='ms')
    flattened_data.set_index('server_time', inplace=True)
    flattened_data.sort_index(inplace=True)

    col_list = flattened_data.columns.to_list()
    col_list = col_list[-1:] + col_list[:-1]

    flattened_data = flattened_data[col_list]

    print(f'Processed {count} of {num_of_sources} devices.')
    print("[ID", internal_id, end=' ')
    print("Device number", str(device_number) + "]")
    print(flattened_data.head(3), flattened_data.tail(3), sep = '\n')
    print('--------------------------------------------------------------------------------------------------------------------------------')
    flattened_data.to_csv(f'./c8y/meas_csv_results/{device_number}.csv')

    count += 1