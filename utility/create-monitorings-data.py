import json
from dateutil.parser import parse

all_meas = []
results = []

# ------------------------------------------------------Edit below------------------------------------------------------
c8y_api_resp_file_prefix = "temp-remove"
c8y_json_file_count = 4
tenant = "t217861659"
# ------------------------------------------------------Edit above------------------------------------------------------

for i in range(c8y_json_file_count):
    with open(f'{c8y_api_resp_file_prefix}-{i}.json') as f:
        # Load JSON data from file
        data = json.load(f)
        # Now data is a Python dictionary
        all_meas.extend(data['measurements'])

for i in range(len(all_meas)):
    print(f"Object ID: {i}\n")
    test_obj = all_meas[i]
    time_in_sec = int(parse(test_obj['time']).timestamp() * 1000)

    new_json = {
        "time":{
            "$date": {
                "$numberLong": str(time_in_sec)
            }
        }, "sensorId": test_obj['source']['id'],
        "type": test_obj['type'],
        "tenant": tenant,
        "reading": test_obj.get('ping') or test_obj.get('measurements')  or test_obj.get('data') or test_obj.get('converged_measurements'),
        "createdAt": {
            "$date": {
                "$numberLong": str(time_in_sec)
            }
        }, "updatedAt":{
            "$date": {
                "$numberLong": str(time_in_sec)
            }
        }
    }

    if new_json['reading'] == None:
        raise ValueError('No reading made!')

    results.append(new_json)

print('Total objects', len(results))
print("----Writing result to file----")
with open('result.json', 'w') as f:
    json.dump(results, f, indent=2)
