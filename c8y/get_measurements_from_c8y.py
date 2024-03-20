import json
import requests
import os
from dotenv import load_dotenv

class MaxRetriesExceededError(Exception):
    """Raised when the maximum number of retries has been exceeded."""
    pass

class DataVerificationError(Exception):
    """Raised when the maximum number of retries has been exceeded."""
    pass


load_dotenv()

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
tenant = "t146989263"
date_to = '2024-03-20T00:00:00.000Z'
date_from = "2024-02-10T00:00:00.000Z"
page_size = "1750"
# Edit here STOP -------------

base_url = f'https://{tenant}.emea.cumulocity.com/inventory/managedObjects'
measurements_url = f'https://{tenant}.emea.cumulocity.com/measurement/measurements'
headers = {
                'User-Agent': 'my-app/0.0.1',
                'Accept': 'application/json',
                'Authorization': os.getenv('AUTH'),
            }

sources = [(source_ids_list[-1], devices_list[-1])]

no_data_found_for_time_range = []

repeat_counter = 0
repeat_until_count = 10


for source_tuple in sources:
    try:
        source = source_tuple[0]
        device_number = source_tuple[1]
        all_data = []
        api_request_page_count = 1

        managed_objects_url = f'{base_url}/{source}'

        response = requests.get(url=managed_objects_url, headers=headers)
        device_name = json.loads(response.text)['name']
        print(f'Device name: {device_name}, device number: {device_number}')
        if str(device_number) not in device_name:
            raise DataVerificationError(f'Device number and name mismatch! Device: {device_number}, Name: {device_name}')

        while True:
                params = {'dateTo': date_to, 'dateFrom': date_from, 'source': source, 'pageSize': page_size, 'currentPage': api_request_page_count}

                print(f'{api_request_page_count}: Making request for {device_number} : {source}', end='')

                # Making the GET request with basic authentication
                response = requests.get(measurements_url, headers=headers, params=params)
                print(' Status', response.status_code)

                if response.status_code < 300:
                    repeat_counter = 0
                    data = json.loads(response.text)

                    if len(data['measurements']) == 0:
                        if api_request_page_count == 1:
                            print('No data found in date range for device number', str(device_number) + ',', 'source:', source)
                            no_data_found_for_time_range.append((device_number, source))
                        break

                    data_list = []
                    for item in data['measurements']:
                        data_list.extend(item['modifiedMeasurements'])
                    all_data.extend(data_list)
                
                    api_request_page_count += 1
                
                else:
                    repeat_counter += 1
                    if repeat_counter > repeat_until_count:
                        print('Reached maximum retries')
                        raise MaxRetriesExceededError(f'Max retries reached for device {device_number}')
        

        print('End. Device number:',device_number, 'Source:', source)
        print('---------------------------------------------------------------------------------------------')
        output_file_path = f'./c8y/meas_json_results/{source}.json'

        with open(output_file_path, 'w') as output_file:
            json.dump(all_data, output_file)
    except KeyboardInterrupt:
        exit()
    except MaxRetriesExceededError as e:
        print(e)
    except DataVerificationError as e:
        print(e)

print('No data for these devices in time range', no_data_found_for_time_range)