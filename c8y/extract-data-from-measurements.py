import pandas as pd
import matplotlib.pyplot as plt
import json
from os import path

# NO DATA FOR THESE (as of 2024-05-13): 63593, 14787, 14912, 70079, 72748, 96168
# DATA WAY FAR BACK (as of 2024-05-15): 50221, 50224, 12483, 98681, 12287

devices_list = [96201, 96168, 98681, 70089, 70091, 70080, 12485, 12483, 12482, 12291, 12287, 50241, 10622, 12108, 10617, 50224, 12495, 12286, 10614, 10616, 70079, 10372, 50221, 12053, 70081, 14773, 14779, 14913, 14787, 72743, 63593, 72748, 14912, 10619, 10620, 12115, 12486, 12490, 63589, 70086, 72759, 72763, 96199, 97405, 12112]

source_ids_list = [389, 3420, 2483, 2508, 3456, 3482, 46964, 47674, 47718, 49492, 61683, 63055, 69952, 69305, 74249, 77532, 152454, 388751, 1049556, 1053103, 1068248, 1128729, 1422168, 2636415, 48727, 6253904087, 3953908687, 29139236, 34695733, 31114007536, 87107442643, 60107407843, 26637896, 70048, 73289, 66930, 2530, 393621, 47107417697, 3467, 51107442290, 37107398393, 3241, 49237155, 66127]

devices_with_no_data = [63593,14787,14912,70079,72748,96168]

try:
    # Edit here START ------------
    devices_of_interest = [96201]
    
    devices_of_interest = list(set(devices_of_interest) - set(devices_with_no_data))
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

    # Add the 'time' column to the flattened DataFrame
    flattened_data['time'] = [item['time'] for item in all_data]

    # Convert 'time' column to datetime
    flattened_data['time'] = pd.to_datetime(flattened_data['time'])
    flattened_data.set_index('time', inplace=True)
    flattened_data.sort_index(inplace=True)

    print(f'Processed {count} of {num_of_sources} devices.')
    print("[ID", internal_id, end=' ')
    print("Device number", str(device_number) + "]")
    print(flattened_data.head(3), flattened_data.tail(3), sep = '\n')
    print('--------------------------------------------------------------------------------------------------------------------------------')
    flattened_data.to_csv(f'./c8y/meas_csv_results/{device_number}.csv')

    count += 1