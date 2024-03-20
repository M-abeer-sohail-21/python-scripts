import json
import requests
import os
from dotenv import load_dotenv
from time import sleep

load_dotenv()

api_url = 'https://secure-2x.circuitmeter.ca'
api_key = os.getenv('CMI_KEY')

health_check_url = f'{api_url}/api/Meters/all?api_key={api_key}'

response = requests.post(url=api_url)

times = [1710461100 + (i * 3600) for i in range(11)]

all_data = []
api_filter = {"filter_key": 'bsid', "filter_value": '3797'}

if(response.status_code < 300):
    datapoints = ['energyimport',
                'amps',
                'PF',
                'volts',
                'frequency',
                'power',
                'apparentpower',
                'reactivepower']
    operators = ['sum',    'avgall',
                'avgall', 'avgall',
                'avgall', 'avgall',
                'avgall', 'avgall']
    for i in range(len(times) - 1):
        for j in range(len(datapoints)):
            body = {
                "datapoint": [
                    datapoints[j]
                ],
                "operator": [
                    operators[j]
                ],
                "filters": { api_filter['filter_key']: [ api_filter['filter_value' ] ] },
                "historical": True,
                "startTime":f"{times[i]}", "endTime":f"{times[i + 1]}",
                "xAxis": 'eid',
                "yAxis": '',
                "time_zone": 'UTC',
                "sortXAxis": 'x',
                "sortYAxis": 'x'
            }
            analytics_url = f'{api_url}/api/ptg?api_key={api_key}'

            response = requests.post(url=analytics_url, json=body)
            data = json.loads(response.text)

            sleep(1)

            measurements_data = data['result']

            if data['success'] == 1:
                all_data.append({
                    "datapoint": datapoints[j],
                    "operator": operators[j],
                    "unit": measurements_data['seriesUnits'],
                    "values": measurements_data['rowData'],
                    "eid": [f'eid-{x}' for x in measurements_data['colID']],
                    "time": int((times[i] + times[i+1])/2)
                })
            else:
                print('Failed for j = ' + j)

print(all_data)
