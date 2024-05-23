from json import load, dumps
from os import getenv
from dotenv import load_dotenv
from time import sleep
from requests import post

load_dotenv()

tenant_token_map = {
    "t1210372600": 'C8Y_E2E_TOKEN',
    "t217861659": "C8Y_RENSAIR_TOKEN"
}
all_data = []

# Edit here START ----------------------------------

endpoint = 'wslistener/populateData'
backend = 'k8s-e2e-backend.xelerate.solutions'
base_path = './utility'
file_path = 'raw_c8y_measurements_to_send_to_wslistener/raw'
tenant = "t217861659"
no_of_files = 2

# Edit here STOP -----------------------------------

token = getenv(tenant_token_map[tenant]).removeprefix('Basic ')

body_obj = {"topic": "MeasurementPopulation", "data": {
    "realtimeAction":"CREATE",
    "tenant":tenant,
    "token": token
}}

full_url = f'https://{backend}/{endpoint}'
print('URL to send at:', full_url,'\n')

for i in range(1 , no_of_files + 1):
    full_file_path = f'{base_path}/{file_path}-{i}.json'
    with open(full_file_path, 'r') as file:
        temp_data = load(file)
        all_data.extend(temp_data['measurements'])

print('Sending data...')

total_data_count = len(all_data)

count = 1

for data in all_data:
    print('Sending object', count, 'of', total_data_count)
    body_obj['data']['data'] = data
    
    response = post(url=full_url, json=body_obj)

    print("\tStatus code:",response.status_code)
    count += 1

    sleep(1)

print('All done!')