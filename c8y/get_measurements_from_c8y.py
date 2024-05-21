import json
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
class MaxRetriesExceededError(Exception):
    """Raised when the maximum number of retries has been exceeded."""
    pass

class DataVerificationError(Exception):
    """Raised when the maximum number of retries has been exceeded."""
    pass

def try_except(func):
    try:
        func()
        return False
    except Exception as e:
        print('[Error]: try_except: caught error:',e)
        return True

load_dotenv()

sources_to_make = []
tenant = "t146989263"
page_size = "1750"

# NO DATA FOR THESE (as of 2024-05-13): 63593, 14787, 14912, 70079, 72748, 96168
# DATA WAY FAR BACK (as of 2024-05-15): 50221, 50224, 12483, 98681, 12287

# TODO: Modify to get device list and source id list from file

devices_list = [96201, 96168, 98681, 70089, 70091, 70080, 12485, 12483, 12482, 12291, 12287, 50241, 10622, 12108, 10617, 50224, 12495, 12286, 10614, 10616, 70079, 10372, 50221, 12053, 70081, 14773, 14779, 14913, 14787, 72743, 63593, 72748, 14912, 10619, 10620, 12115, 12486, 12490, 63589, 70086, 72759, 72763, 96199, 97405, 12112]

source_ids_list = [389, 3420, 2483, 2508, 3456, 3482, 46964, 47674, 47718, 49492, 61683, 63055, 69952, 69305, 74249, 77532, 152454, 388751, 1049556, 1053103, 1068248, 1128729, 1422168, 2636415, 48727, 6253904087, 3953908687, 29139236, 34695733, 31114007536, 87107442643, 60107407843, 26637896, 70048, 73289, 66930, 2530, 393621, 47107417697, 3467, 51107442290, 37107398393, 3241, 49237155, 66127]

devices_with_no_data = [63593,14787,14912,70079,72748,96168]

try:
    # Edit here START ------------
    devices_of_interest = [12112]
    # Edit here STOP -------------

    devices_of_interest = list(set(devices_of_interest) - set(devices_with_no_data))
    
    not_found_devices = [x for x in devices_of_interest if try_except(lambda x=x,y=devices_list,z=source_ids_list: z[y.index(x)])]

    if len(not_found_devices) != 0:
        raise ValueError(f'Source IDs for these device numbers not found! {not_found_devices}')

    sources_to_make = [devices_list.index(x) for x in devices_of_interest]

    while True:
        enable_time_frame = input('A time frame is set? (y/n) ')
        if enable_time_frame in ['yes', 'no', 'y', 'n']:
            if enable_time_frame in ['yes', 'y']:
                enable_time_frame = True
            else:
                enable_time_frame = False
            break

except ValueError as e:
    print('[Error]: Device number not found!', end=' ')
    print(e)
    exit()

if enable_time_frame:
    days_to_go_back = int(input('Enter number of days to go back: '))
    now = datetime.now(timezone.utc)
    date_to = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    date_from = (now - timedelta(days=days_to_go_back)).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    print(f'Date from: {date_from}, Date to: {date_to}\n')
# Edit here STOP -------------

managed_obj_url = f'https://{tenant}.emea.cumulocity.com/inventory/managedObjects'
measurements_url = f'https://{tenant}.emea.cumulocity.com/measurement/measurements'
headers = {
                'User-Agent': 'my-app/0.0.1',
                'Accept': 'application/json',
                'Authorization': os.getenv('AUTH'),
            }

sources = [(source_ids_list[i], devices_list[i]) for i in sources_to_make]

devices_no_data = []

repeat_counter = 0
repeat_until_count = 10

total_devices_count = len(sources)

for i in range(total_devices_count):
    try:
        source = sources[i][0]
        device_number = sources[i][1]
        all_data = []
        api_request_page_count = 1

        managed_objects_url = f'{managed_obj_url}/{source}'

        response = requests.get(url=managed_objects_url, headers=headers)
        device_name = json.loads(response.text)['name']

        print(f'Processing device {i+1} of {total_devices_count}')
        print(f'Device name: {device_name}, device number: {device_number}')
        if str(device_number) not in device_name:
            devices_no_data.append(device_number)
            raise DataVerificationError(f'Device number and name mismatch! Device: {device_number}, Name: {device_name}')

        while True:
                params = {'source': source, 'pageSize': page_size, 'currentPage': api_request_page_count}

                if enable_time_frame:
                    params['dateTo'] = date_to
                    params['dateFrom'] = date_from

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
                            devices_no_data.append((device_number, source))
                        break

                    data_list = []
                    for item in data['measurements']:
                        type_measurement = item['type']
                        if type_measurement == 'measurements':
                            data_list.append({'payload': item[type_measurement], 'time': item['time']})
                        elif type_measurement == 'modifiedMeasurements':
                            data_list.extend(item[type_measurement])
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
        print('[Error]: ', end = '')
        print(e)
        devices_no_data.append(device_number)
    except DataVerificationError as e:
        print('[Error]: ', end = '')
        print(e)
        devices_no_data.append(device_number)

if (devices_no_data != []):
    print('No data for these devices in time range:')
    for device in devices_no_data:
        print(device)