# DISCLAIMER: RUN THIS SCRIPT WITH EDITS AND IF YOU KNOW WHAT YOU"RE DOING!!!

# TODO: Verify device name to change is correct by comparing with bike number

import pandas as pd
import requests as req
import os
import json
from dotenv import load_dotenv

load_dotenv()

base_path = '/home/sarwan/Downloads'
file_path = ''
full_path = f'{base_path}/{file_path}'

data = pd.read_csv(full_path, index_col='internalId', low_memory=False)

tenant = "t146989263"
page_size = "1750"
managed_obj_url = f'https://{tenant}.emea.cumulocity.com/inventory/managedObjects'
auth_token = os.getenv('C8Y_ABDS_TOKEN')

headers = {
                'User-Agent': 'python/0.0.1',
                'Accept': 'application/json',
                'Authorization': auth_token,
            }

for source in list(data.index):
    if (source not in ['12495', '-']):
        managed_obj_url_device = f'{managed_obj_url}/{source}'

        response_get = req.get(url=managed_obj_url_device, headers=headers)
        device_name = json.loads(response_get.text)['name']

        new_device_name = f'[PARKED] - {device_name}'

        response_put = req.put(url=managed_obj_url_device, headers=headers, json = {'name': new_device_name})
        print(json.loads(response_put.text)['name'])