import json
import requests
import os
from dotenv import load_dotenv
from time import sleep
from data import *
from datetime import datetime
from functools import reduce
import pytz

gmt_timezone = pytz.timezone('GMT')

load_dotenv()

# CHECKME: Very unstable!

all_data = []

data_to_send_all = []

c8y_url = 'https://lla-rnd.emea.cumulocity.com/measurement/measurements'
auth = os.getenv('LLA_AUTH')

def generate_device_data(device_id, time, payload):
    
    return {
        "type": "converged_measurements",
        "time" : time.isoformat(timespec='milliseconds') + 'Z',
        "source": {"id":device_id },
        "converged_measurements":payload
    }

for a in data_to_process:
    for i in range(len(a['eid'])):
        all_data.append(
            {
             "eid": a['eid'][i],
             "datapoint": a['datapoint'],
             "unit": a['unit'][0],
             "value": a['values'][0][i],
             "time": datetime.utcfromtimestamp(a['time'])
        })

device_id_mappings = [{x['deviceId']: x['c8ySourceIds'][0]['v']} for x in device_identities]
device_id_mappings = reduce(lambda a, b: {**a, **b}, device_id_mappings)
datapoint_mappings = [{v: k} for k, v in datapoint_mappings.items()]
datapoint_mappings = reduce(lambda a, b: {**a, **b}, datapoint_mappings)

for data_packet in all_data:
    c8y_id = device_id_mappings[data_packet['eid']]
    time = data_packet['time']
    payload = {
        datapoint_mappings[data_packet['datapoint']]: {"value": data_packet['value'], "unit": data_packet['unit']}
    }
    data_to_send = generate_device_data(c8y_id, time, payload)

    data_to_send_all.append(data_to_send)

n = 8
print(len(data_to_send_all))
data_to_send_all = [data_to_send_all[i:i+n] for i in range(0, len(data_to_send_all), n)]

print(data_to_send_all[0])

for lst in data_to_send_all[0]:
    response = requests.post(c8y_url, headers = {
                    'User-Agent': 'my-app/0.0.1',
                    'Accept': 'application/json',
                    'Authorization': os.getenv('LLA_AUTH')
                }, json={
                    "measurements": lst
                })
    print(response.status_code)
    sleep(1)