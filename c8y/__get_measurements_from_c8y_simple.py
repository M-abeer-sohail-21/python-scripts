import json
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from math import log10

load_dotenv()

auth_token = ''

# Edit here START ------------
devices_of_interest = [27127899218,70167043860,84167050702,62167050371,29167051015,87167051119,65167821302,77167816025]
auth_token = os.getenv('C8Y_ABDS_TOKEN')
tenant = "t146989263"
page_size = "1750"
date_to = "2024-07-28T15:17:46.000Z"
date_from = "2024-07-27T19:00:17.000Z"
enable_time_frame = True
base_file_path = "./c8y/meas_json_results_simple/abds"
# Edit here STOP -------------

measurements_url = f'https://{tenant}.emea.cumulocity.com/measurement/measurements'
headers = {
                'User-Agent': 'my-app/0.0.1',
                'Accept': 'application/json',
                'Authorization': auth_token,
            }

devices_no_data = []
file_count = 0

for i in range(len(devices_of_interest)):
    
    source = devices_of_interest[i]

    max_retry_count = 0

    api_request_page_count = 1

    try:        

        params = {'source': source, 'pageSize': page_size, 'currentPage': api_request_page_count, "revert": True}

        if enable_time_frame:
            params['dateTo'] = date_to
            params['dateFrom'] = date_from
        
        response = requests.get(url=measurements_url, headers=headers, params=params)

        print(f'Device ID: {source}')

        while True:
            try:                

                print(f'{api_request_page_count}: Making request for {source}')

                # Making the GET request with basic authentication
                params['currentPage'] = api_request_page_count
                response = requests.get(measurements_url, headers=headers, params=params)
                print('Status', response.status_code)

                if max_retry_count > 5:
                    raise Exception('Max retries reached.')
                
                if response.status_code < 300:
                    data = json.loads(response.text)

                    if len(data['measurements']) == 0:
                        if api_request_page_count == 1:
                            print('No data found in date range for device number', str(source) + ',', 'source:', source)
                            devices_no_data.append(source)
                        break

                    with open(f'{base_file_path}-{file_count}.json', 'w') as f:
                        json.dump(data, f, indent=4)
                
                    api_request_page_count += 1
                    file_count += 1
                
                else:
                    max_retry_count += 1

            except Exception as e:
                choice = input(f'Unexpected exception encountered: {e}. Exit? (y)')
                if choice.lower() == 'y':
                    exit()
                else:
                    pass

        print('---------------------------------------------------------------------------------------------')
        
    except KeyboardInterrupt:
        exit()

if (devices_no_data != []):
    print('No data for these devices in time range:')
    for device in devices_no_data:
        print(device)