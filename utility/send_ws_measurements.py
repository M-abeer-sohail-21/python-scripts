from json import load, dumps
from os import getenv
from dotenv import load_dotenv
from time import sleep
from requests import post

load_dotenv()

tenant_token_map = {
    "t1210372600": 'C8Y_E2E_TOKEN',
    "t217861659": "C8Y_RENSAIR_TOKEN",
    "t160245771": "C8Y_ORANO_TOKEN",
    "t146989263": "C8Y_ABDS_TOKEN"
}

tenant_url_map = {
    "t160245771": "k8s-orano-backend.xelerate.solutions", # TODO: CHANGE to Azure
    "t217861659": "k8s-e2e-backend.xelerate.solutions", # TODO: CHANGE to Azure
    "t146989263": "albusayra-backend.xelerate.digital"
}

all_data = []

# Edit here START ----------------------------------

# The raw data is taken directly from c8y JSON data

endpoint = 'wslistener/populateData'
base_path = './utility'
file_path = 'raw_c8y_measurements_to_send_to_wslistener'
tenant = "t217861659"
no_of_files = 2
prefix = "rensair"

# Edit here STOP -----------------------------------

backend = tenant_url_map[tenant]
token = getenv(tenant_token_map[tenant]).removeprefix('Basic ')

body_obj = {"topic": "MeasurementPopulation", "data": {
    "realtimeAction":"CREATE",
    "tenant":tenant,
    "token": token
}}

full_url = f'https://{backend}/{endpoint}'
print('URL to send at:', full_url,'\n')

for i in range(1 , no_of_files + 1):
    full_file_path = f'{base_path}/{file_path}/{prefix}-raw-{i}.json'
    with open(full_file_path, 'r') as file:
        temp_data = load(file)
        all_data.extend(temp_data['measurements'])

print('Sample data:')
print(dumps(all_data[0], indent=4))

total_data_count = len(all_data)
print('Total data:', total_data_count)

print('\nSending data...')
sleep(2)

count = 1

for data in all_data:
    print('Sending object', count, 'of', total_data_count)
    body_obj['data']['data'] = data
    
    response = post(url=full_url, json=body_obj)

    print("\tStatus code:",response.status_code)
    count += 1

    sleep(1)

print('All done!')