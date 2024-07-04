import pandas as pd
import matplotlib.pyplot as plt
import json
from os import path

# NO DATA FOR THESE (as of 2024-05-13): 63593, 14787, 14912, 70079, 72748, 96168, 11865
# DATA WAY FAR BACK (as of 2024-05-15): 50221, 50224, 12483, 98681, 12287

# TODO: Modify to get device list and source id list from file

devices_list = [96201, 96168, 98681, 12692, 12278, 12270, 12485, 12483, 12482, 12687, 98253, 12273, 10622, 12108, 10617, 50224, 12495, 11865, 10614, 10616, 12276, 10372, 50221, 12084, 12693, 14773, 14779, 14913, 14787, 72743, 63593, 72748, 14912, 10619, 10620, 12609, 12486, 12490, 63589, 12694, 72759, 72763, 96199, 97405, 12245, 12075, 12269, 12279]

source_ids_list = [389, 3420, 2483, 2508, 3456, 3482, 46964, 47674, 47718, 49492, 61683, 63055, 69952, 69305, 74249, 77532, 152454, 388751, 1049556, 1053103, 1068248, 1128729, 1422168, 2636415, 48727, 6253904087, 3953908687, 29139236, 34695733, 31114007536, 87107442643, 60107407843, 26637896, 70048, 73289, 66930, 2530, 393621, 47107417697, 3467, 51107442290, 37107398393, 3241, 49237155, 66127, 49386, 337216, 3464]

auth_token = ''

try:
    # Edit here START ------------
    devices_of_interest = [96201, 96168, 98681, 12279, 12270, 12485, 12483, 12482, 12075, 98253, 12245, 10619, 10620, 10622, 12108, 50224, 12495, 11865, 12276, 10372, 50221, 97405, 14773, 14779, 14913, 14787, 72743, 63593, 72748, 14912, 12692, 12687, 12693]

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