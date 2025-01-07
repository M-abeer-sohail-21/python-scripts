import json
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from ignore_constants import *
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

def has_duplicates(input_list):
    if not isinstance(input_list, list):
        raise ValueError("Input must be a list")
    
    return len(input_list) != len(set(input_list))

# Edit here START --------------------------------

skip_duplicate_check = True

# Edit here END ----------------------------------

load_dotenv()

sources_to_make = []
tenant = "t146989263"
page_size = "1750"

# NO DATA FOR THESE (as of 2024-05-13): 63593, 14787, 14912, 70079, 72748, 96168, 11865
# DATA WAY FAR BACK (as of 2024-05-15): 50221, 50224, 12483, 98681, 12287
# NAME CHANGE on 2024-07-03. Check code history

auth_token = ''

if not skip_duplicate_check:
    if has_duplicates(devices_list) or has_duplicates(source_ids_list):
        raise ValueError(f'Either devices list or source_ids_list contains duplicates: device_list {has_duplicates(devices_list)} source_ids_list {has_duplicates(source_ids_list)}')

try:
    # Edit here START ------------    
    auth_token = os.getenv('C8Y_ABDS_TOKEN')
    year = 2024
    month = 8
    day = 5
    # Edit here STOP -------------
    
    # This line is not relevant any more
    # not_found_devices = [x for x in devices_of_interest if try_except(lambda x=x,y=devices_list,z=source_ids_list: z[y.index(x)])]
    # if len(not_found_devices) != 0:
    #     raise ValueError(f'Source IDs for these device numbers not found! {not_found_devices}')

    sources_to_make = [devices_list.index(x) for x in devices_list]

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
    zulu_time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    # now = datetime.now(timezone.utc)
    # date_to = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    # date_from = (now - timedelta(days=days_to_go_back)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    date_to = datetime.strptime(f'{year}-{str(month).zfill(2)}-{str(day).zfill(2)}T00:00:00.000Z', zulu_time_format)
    days_to_go_back = int(input('Enter number of days to go back: '))
    date_from = date_to - timedelta(days = days_to_go_back)

    if date_to - date_from < timedelta(minutes=1):
        raise ValueError('date_from is greater than date_to!')
    
    date_from = datetime.strftime(date_from, zulu_time_format)
    date_to = datetime.strftime(date_to, zulu_time_format)

    print(f'Date from: {date_from}, Date to: {date_to}\n')
# Edit here STOP -------------

managed_obj_url = f'https://{tenant}.emea.cumulocity.com/inventory/managedObjects'
measurements_url = f'https://{tenant}.emea.cumulocity.com/measurement/measurements'
headers = {
                'User-Agent': 'my-app/0.0.1',
                'Accept': 'application/json',
                'Authorization': auth_token,
            }

sources = [(source_ids_list[i], devices_list[i]) for i in sources_to_make]

devices_no_data = []

total_devices_count = len(sources)

for i in range(total_devices_count):
    try:
        source = sources[i][0]
        device_number = sources[i][1]
        all_data = []
        api_request_page_count = 1

        managed_objects_url = f'{managed_obj_url}/{source}'

        response = requests.get(url=managed_objects_url, headers=headers)

        json_obj = json.loads(response.text)

        if "error" in json_obj.keys():
            raise DataVerificationError(f'Error received: {json_obj["error"]["message"]}')

        device_name = json.loads(response.text)['name']

        print(f'Processing device {i+1} of {total_devices_count}')
        print(f'Device name: {device_name}, device number: {device_number}')
        if str(device_number) not in device_name:
            devices_no_data.append(device_number)
            raise DataVerificationError(f'Device number and name mismatch! Device: {device_number}, Name: {device_name}')
        
        max_retry_count = 0

        while True:
            try:
                params = {'source': source, 'pageSize': page_size, 'currentPage': api_request_page_count, "revert": True}

                if enable_time_frame:
                    params['dateTo'] = date_to
                    params['dateFrom'] = date_from

                print(f'{api_request_page_count}: Making request for {device_number} : {source}', end='')

                # Making the GET request with basic authentication
                response = requests.get(measurements_url, headers=headers, params=params)
                print(' Status', response.status_code)

                if response.status_code < 300:
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
                    max_retry_count += 1

                    if max_retry_count > 5:
                        raise MaxRetriesExceededError('Max retries for API call exceeded with 4XX status')

            except Exception as e:
                choice = input(f'Unexpected exception encountered: {e}. Exit? (y)')
                if choice.lower() == 'y':
                    exit()
                else:
                    pass

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
    device_numbers_with_no_data = ''
    source_numbers_with_no_data = ''
    for device in devices_no_data:
        device_numbers_with_no_data += device[0] + ", "
        source_numbers_with_no_data += device[1] + ", "
    
    print('Device numbers\n-------------------------')
    print(device_numbers_with_no_data)
    print('Source numbers\n-------------------------')
    print(source_numbers_with_no_data)