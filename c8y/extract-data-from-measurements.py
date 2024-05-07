import pandas as pd
import matplotlib.pyplot as plt
import json
from os import path

devices_list = [63589, 70091, 63593, 12053, 12485, 12483, 70079, 17018, 14912, 
              17020, 50221, 96168, 98681, 14787, 97405, 12108, 72748, 50224, 
              70080, 96199, 14913, 17019, 70089, 12287, 72763, 10616, 12286, 70086, 50241, 12115, 10620, 10622, 10617, 12495, 14779, 72759, 12486, 12291,
12482,
14773,
70080,
70081,
70091,
72743,
96201, 12490, 35759]
source_ids_list = [47107417697, 3456, 87107442643, 2636415, 46964, 47674, 1068248, 2002381, 26637896,
                   439804, 1422168, 3420, 2483, 34695733, 49237155, 69305, 60107407843, 77532,
                   3482, 3241, 29139236, 434557, 2508, 61683, 37107398393, 1053103, 388751, 3467, 63055, 66930, 73289, 69952, 74249, 152454, 3953908687, 51107442290, 2530,49492,
47718,
6253904087,
3482,
48727,
3456,
31114007536,
389, 393621, 11127894796]

# No data from 01 Jan onwards: 12483 - 96168
# Online but danger: 63589, 97405, 14913
# No data: 72748 (Did not connect after installation)
# No data on c8y: 12483, 70079, 17018, 14912, 17020, 96168

sources_to_make = []
tenant = "t146989263"
page_size = "1750"

try:
    # Edit here START ------------
    devices_of_interest = [12490, 12291, 12482, 14773, 70080, 70081, 70091, 72743, 96201, 10617, 10620, 10622, 12495, 14779, 50241, 72759, 12115, 12486, 12286, 70086, 10616, 12287, 72763, 70089, 63589, 12053, 12108, 12485, 12483, 14787, 14912, 14913, 50221, 70091, 50224, 63593, 70079, 72748, 96199, 96168, 97405, 98681, 35759]
    # Edit here STOP -------------
    sources_to_make = [devices_list.index(x) for x in devices_of_interest]

except ValueError as e:
    print('[Error]: Device number not found!', end=' ')
    print(e)
    exit()

sources = [(source_ids_list[i], devices_list[i]) for i in sources_to_make]

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

    print("ID", internal_id)
    print("Device number", device_number)
    print(flattened_data.head(3), flattened_data.tail(3), sep = '\n')
    print('--------------------------------------------------------------------------------------------------------------------------------')
    flattened_data.to_csv(f'./c8y/meas_csv_results/{device_number}.csv')

    