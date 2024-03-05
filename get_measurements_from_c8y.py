import json
import requests

all_data = []

api_request_page_count = 1

while True:
    # Replace 'your_username' and 'your_password' with your actual credentials
    source = "34695733"
    tenant = "t146989263"
    date_to = '2024-02-09T00:00:00.000Z'
    date_from = "2024-01-18T00:00:00.000Z"
    page_size = "1500"
    url = f'https://{tenant}.emea.cumulocity.com/measurement/measurements?dateTo={date_to}&pageSize={page_size}&source={source}&dateFrom={date_from}&currentPage={api_request_page_count}'
    headers = {
        'User-Agent': 'my-app/0.0.1',
        'Accept': 'application/json',
        'Authorization': 'Basic dDE0Njk4OTI2My9hYmVlci5zb2hhaWxAaW52aXhpYmxlLmNvbTo1SHpYeHp2XzcuLVZmMzU=',
    }

    print(f'{api_request_page_count}: Making request at {url}')

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

output_file_path = f'meas_json_results/{source}.json'

with open(output_file_path, 'w') as output_file:
    json.dump(all_data, output_file)