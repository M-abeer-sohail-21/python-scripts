from json import load, dumps
from os import getenv
from dotenv import load_dotenv
from time import sleep
from requests import post, HTTPError

load_dotenv()

# TODO Use XLR8 API to get the access token

tenants = {
   "e2e":{
      "tenant_id_variable":"C8Y_E2E_TENANT_ID",
      "token_variable":"C8Y_E2E_TOKEN",
      "x_access_token_variable": "C8Y_E2E_X_ACCESS_TOKEN",
      "backend_url_variable":"C8Y_E2E_BACKEND_URL"
   }, "rensair":{
      "tenant_id_variable":"C8Y_RENSAIR_TENANT_ID",
      "token_variable":"C8Y_RENSAIR_TOKEN",
      "x_access_token_variable": "C8Y_RENSAIR_X_ACCESS_TOKEN",
      "backend_url_variable":"C8Y_RENSAIR_BACKEND_URL"
   }
}

all_data = []

# Edit here START ----------------------------------

# The raw data is taken directly from c8y API measurements JSON data

base_path = '.\\utility'
file_path = 'raw_c8y_measurements_to_send_to_wslistener'
environment = "rensair"
no_of_files = 6 # index files starting at 0
prefix = "rensair-raw"
start_from_index = 8747 + 282 - 2 # make it None by default
end_till_index = None # make it None by default
start_sending_data = True

# Edit here STOP -----------------------------------

endpoint = 'wslistener/populateData'

backend_url = getenv(tenants[environment]["backend_url_variable"])
token = getenv(tenants[environment]['token_variable']).removeprefix('Basic ')
tenant_id = getenv(tenants[environment]["tenant_id_variable"])
x_access_token = getenv(tenants[environment]["x_access_token_variable"])

body_obj = {"topic": "MeasurementPopulation", "data": {
    "realtimeAction":"CREATE",
    "tenant":tenant_id,
    "token": token,
    "data": {
        "data": None
    }
}}

headers = {
    "x-access-token": x_access_token
}

full_url = f'{backend_url}/{endpoint}'
print('URL to send at:', full_url,'\n')

for i in range(no_of_files):
    full_file_path = f'{base_path}\\{file_path}\\{prefix}-{str.zfill(str(i),2)}.json'
    with open(full_file_path, 'r') as file:
        temp_data = load(file)
        all_data.extend(temp_data['measurements'])

print('Sample data:')
print(dumps(all_data[0], indent=4))

all_data = all_data[start_from_index:end_till_index]
total_data_count = len(all_data)
print('Total data:', total_data_count)

if start_sending_data:

    print('\nSending data...')
    sleep(2)

    count = 1

    for data in all_data:
        print('Sending object', count, 'of', total_data_count)
        body_obj['data']['data'] = data
        
        response = post(url=full_url, json=body_obj)

        print("\tStatus code:",response.status_code)

        if response.status_code >= 400:
            raise HTTPError(f'Error code received for count: {count}: {response.status_code}')

        count += 1

        sleep(1)

print('All done!')