import json
import datetime
import dateutil

base_path = './temp'

def process_data(no_of_files):
    global base_path

    data = []
    all_lines = []

    for i in range(1, no_of_files + 1):
        file_path = f'{base_path}/raw-{i}.json'

        with open(file_path, 'r') as f:
            temp_data = json.load(f)
            data.extend(temp_data['events'])
    
    with open('results.txt', 'w') as f:
        for obj in data:
            if len(obj):
                print(obj)
                for i in range(len(obj['data'])):
                    line = f'time: {datetime.datetime.strptime(obj["data"][i]["time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(hours=4)}, latitude: {obj["data"][i]["c8y_Position"]["lat"]}, longitude: {obj["data"][i]["c8y_Position"]["lng"]}\n'
                    time = datetime.datetime.strptime(obj["data"][i]["time"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(hours=4)
                    if time.hour >= 0 and time.hour <= 2:
                        f.write(line)

process_data(2)