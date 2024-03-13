import json
import requests
import os
from dotenv import load_dotenv

devices_list = [63589, 70091, 63593, 12053, 12485, 12483, 70079, 17018, 14912, 
              17020, 50221, 96168, 98681, 14787, 97405, 12108, 72748, 50224, 
              70080, 96199, 14913]
source_ids_list = [47107417697, 3456, 87107442643, 2636415, 46964, 47674, 1068248, 2002381, 26637896,
                   439804, 1422168, 3420, 2483, 34695733, 49237155, 69305, 60107407843, 77532,
                   3482, 3241, 29139236]

# No data from 01 Jan onwards: 12483 - 14912, 17020, 50221, 96168, 72748
# Online but danger: 63589, 97405, 14913 

# Edit here START ------------
sources_to_make = list(range(devices_list.index(17020), devices_list.index(50224) + 1))
# Edit here STOP -------------

sources = [(source_ids_list[i], devices_list[i]) for i in sources_to_make]

no_data_found_for_time_range = []

load_dotenv()

for source_tuple in sources:
    source = source_tuple[0]
    bike_number = source_tuple[1]
    all_data = []
    api_request_page_count = 1
    while True:
        try:
            # Replace 'your_username' and 'your_password' with your actual credentials
            tenant = "t146989263"
            date_to = '2024-03-13T00:00:00.000Z'
            date_from = "2024-01-01T00:00:00.000Z"
            page_size = "1500"
            
            headers = {
                'User-Agent': 'my-app/0.0.1',
                'Accept': 'application/json',
                'Authorization': os.getenv('AUTH'),
            }

            url = f'https://{tenant}.emea.cumulocity.com/measurement/measurements'

            params = {'dateTo': date_to, 'dateFrom': date_from, 'source': source, 'pageSize': page_size, 'currentPage': api_request_page_count}

            print(f'{api_request_page_count}: Making request for {bike_number}:{source}')

            # Making the GET request with basic authentication
            response = requests.get(url, headers=headers, params=params)
            print('  Status', response.status_code)

            if response.status_code < 300:
                data = json.loads(response.text)

                if len(data['measurements']) == 0:
                    if api_request_page_count == 1:
                        print('No data found in date range for bike', str(bike_number) + ',', 'source:', source)
                        no_data_found_for_time_range.push(bike_number)
                    break

                data_list = []
                for item in data['measurements']:
                    data_list.extend(item['modifiedMeasurements'])
                all_data.extend(data_list)
            
                api_request_page_count += 1
            
        except KeyboardInterrupt:
            exit()
        except Exception as e:
            print(f'An exception has occured {e}, repeating request.')

        

    print('End. Bike:',bike_number, 'Source:', source)
    print('---------------------------------------------------------------------------------------------')
    output_file_path = f'./c8y/meas_json_results/{source}.json'

    print('No data for these devices', no_data_found_for_time_range)

    with open(output_file_path, 'w') as output_file:
        json.dump(all_data, output_file)