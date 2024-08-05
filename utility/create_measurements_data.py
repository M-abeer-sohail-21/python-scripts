import json
from dateutil.parser import parse
from time import sleep

all_meas = []
results = []

# TODO: Check create_test_data to see how to make it more unified and modular

# ------------------------------------------------------Edit below------------------------------------------------------
main_folder = "utility"
c8y_api_resp_file_prefix = main_folder + "/raw-meas-jsons/raw"
c8y_json_file_count = 2
tenant = "t217861659"
# ------------------------------------------------------Edit above------------------------------------------------------

for i in range(1, c8y_json_file_count + 1):
    with open(f'{c8y_api_resp_file_prefix}-{i}.json') as f:
        # Load JSON data from file
        data = json.load(f)
        # Now data is a Python dictionary
        all_meas.extend(data['measurements'])

print('Processing...\n')
sleep(2)

total_count = len(all_meas)

for i in range(total_count):
    print(f"...object number: {i + 1} of {total_count}")
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
        "reading": test_obj.get(test_obj.get('type')),
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

print('Tenant:', tenant)
print('Total event objects processed:', len(results))
print("Writing result to file...")
with open(main_folder + '/result.json', 'w') as f:
    json.dump(results, f, indent=2)
