import json
import requests
import os
from dotenv import load_dotenv

sources = ["34695733", "49237155", "2483"]

load_dotenv()

for source in sources:
    all_data = []
    api_request_page_count = 1
    while True:
        # Replace 'your_username' and 'your_password' with your actual credentials
        tenant = "t146989263"
        date_to = '2024-02-09T00:00:00.000Z'
        date_from = "2024-01-18T00:00:00.000Z"
        page_size = "1500"
        url = f'https://{tenant}.emea.cumulocity.com/measurement/measurements?dateTo={date_to}&pageSize={page_size}&source={source}&dateFrom={date_from}&currentPage={api_request_page_count}'
        headers = {
            'User-Agent': 'my-app/0.0.1',
            'Accept': 'application/json',
            'Authorization': os.getenv('AUTH'),
        }

        print(f'{api_request_page_count}: Making request for {source}')

        # Making the GET request with basic authentication
        response = requests.get(url, headers=headers)

        if response.status_code < 300:
            data = json.loads(response.text)

            if len(data['measurements']) == 0:
                break

            data_list = []
            for item in data['measurements']:
                data_list.extend(item['modifiedMeasurements'])
            all_data.extend(data_list)

        api_request_page_count += 1

    print('Source:', source)
    print('---------------------------------------------------------------------------------------------')
    output_file_path = f'meas_json_results/{source}.json'

    with open(output_file_path, 'w') as output_file:
        json.dump(all_data, output_file)