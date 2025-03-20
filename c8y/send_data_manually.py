from os import listdir
from json import loads, dumps, load
from random import random
from datetime import datetime
from requests import post, HTTPError
from time import sleep

# TODO as of 20 Mar 2025
# Historical time is implemented, needs more work for sending real time data
# Make function more mature

times_to_consider = {
    "years": [2025],
    "months": [3],
    "days": list(range(14,19)),
    "hours": list(range(23,24)),
    "minutes": list(range(0,60,30))
}

device_id = "117987784"
url = "https://demo-solutions.emea.cumulocity.com/"
endpoint = "measurement/measurements"
auth_header = "Basic dDEyMTAzNzI2MDAvbm9kZXJlZF9zaW1zX2Z1bmM6Qmxhc3RlcnNfMjAyMiE="

def generate_random_data(min, max, dp):
    random_base_val = random()*(max-min) + min
    power_of_ten = 10**dp
    return int(random_base_val * power_of_ten) / power_of_ten

def minutes_from_midnight():
    now = datetime.now()
    then = datetime(now.year, now.month, now.day, 0,0,0)
    diff = (now-then).total_seconds()
    return diff/1000/60; 

def generate_device_data_historical(device_id, year, month, day, hour, minute, second):
    return_obj = {
                "type":"converged_measurements",
                "time":f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}T{str(hour).zfill(2)}:{str(minute).zfill(2)}:{str(second).zfill(2)}.000Z",
                "source": {"id":device_id },
                "converged_measurements":{
                    "Temperature": { "value": generate_random_data(16,35,2), "unit": "°C" },
                    "Humidity": { "value": generate_random_data(40,70,2), "unit": "%" },
                    "Pressure": { "value": generate_random_data(0,2,2), "unit": "bar" },
                    "LightIntensity": { "value": generate_random_data(0,200,0), "unit": "Lux" },
                    "CO2": { "value": generate_random_data(400,1200,0), "unit": "µg/m3" },
                    "CO": { "value": generate_random_data(0,2,2), "unit": "µg/m3/10" },
                    "Smoke": { "value": 0, "unit": "µg/m3" },
                    "Fire": { "value": 0, "unit": "µg/m3" },
                    "CurrentOccupants": { "value": generate_random_data(10,20,0), "unit": "count" },
                    "TotalFootfall": { "value": int(minutes_from_midnight()/20), "unit": "count" },
                    "AQI_Range": { "value": generate_random_data(0,200,0), "unit": "AQI" }
               }
            }
    return return_obj

count = 1

for year in times_to_consider['years']:
    for month in times_to_consider['months']:
        for day in times_to_consider['days']:
            for hour in times_to_consider['hours']:
                for minute in times_to_consider['minutes']:
                    meas_obj = generate_device_data_historical(device_id, year, month, day, hour, minute, 0)
                    request_body = {"measurements":[meas_obj]}
                    response_obj = post(f'{url}/{endpoint}', json=request_body, headers={
                        'User-Agent': 'my-app/0.0.1',
                        'Accept': 'application/json',
                        'Content-Type': 'application/vnd.com.nsn.cumulocity.measurementCollection+json',
                        'Authorization': auth_header,
                    })
                    print(f'*=*=*=*=*=*=*=* Sending for {f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}T{str(hour).zfill(2)}:{str(minute).zfill(2)}:00.000Z"}')
                    if response_obj.status_code >= 400:
                        raise HTTPError(f'Error when POSTing data: {response_obj.status_code}')
                    print(f'-+-+-+-+-+-+-+- Sent successfully for {f"{year}-{str(month).zfill(2)}-{str(day).zfill(2)}T{str(hour).zfill(2)}:{str(minute).zfill(2)}:00.000Z"}\n')
                    sleep(2)
                    
