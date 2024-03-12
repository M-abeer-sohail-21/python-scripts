import json
import requests
import os
from dotenv import load_dotenv

sources = [('3241', '96199')]

# ('3456', '70091'), ('46964', '12485'), , ('77532', '50224'), ('1068248', '70079'), ('2002381', '17018'), ('2636415', '12053'), ('87107442643', '63593')
# -> Nothing for 47674, 1068248, 2002381

load_dotenv()

for source_tuple in sources:
    source = source_tuple[0]
    all_data = []
    api_request_page_count = 1
    while True:
        try:
            # Replace 'your_username' and 'your_password' with your actual credentials
            tenant = "t146989263"
            date_to = '2024-03-06T00:00:00.000Z'
            date_from = "2024-01-18T00:00:00.000Z"
            page_size = "1500"
            url = f'https://{tenant}.emea.cumulocity.com/measurement/measurements'
            headers = {
                'User-Agent': 'my-app/0.0.1',
                'Accept': 'application/json',
                'Authorization': os.getenv('AUTH'),
            }

            params = {'dateTo': date_to, 'dateFrom': date_from, "pageSize": page_size, "currentPage": api_request_page_count}

            print(f'{api_request_page_count}: Making request for {source}')

            # Making the GET request with basic authentication
            response = requests.get(url, headers=headers, params=params)

            if response.status_code < 300:
                data = json.loads(response.text)

                if len(data['measurements']) == 0:
                    break

                data_list = []
                for item in data['measurements']:
                    data_list.extend(item['modifiedMeasurements'])
                all_data.extend(data_list)
            
            api_request_page_count += 1
            
        except:
            print('An exception has occured, repeating request.')

        

    print('End. Source:', source)
    print('---------------------------------------------------------------------------------------------')
    output_file_path = f'meas_json_results/{source}.json'

    with open(output_file_path, 'w') as output_file:
        json.dump(all_data, output_file)