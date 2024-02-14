from datetime import datetime, timedelta
import json

def get_time(obj):
        time_int = int(float(obj['time']['$date']['$numberLong'])/1000)
        return datetime.fromtimestamp(time_int)

def get_value_within_bounds(msg, min_val, max_val):
        val = None
        while True:
                val = int(input(msg + " "))
                if val < min_val or val > max_val:
                        print('ERROR! Value must be between', min_val, 'and', 'max_val')
                else:
                        break
        
        return val

def compare_properties(obj1, obj2, prop):
        value_match = obj1[prop]['value'] == obj2[prop]['value']
        unit_match = obj1[prop]['unit'] == obj2[prop]['unit']
        return value_match and unit_match

def sort_object_list_using_nested_property(lst, property_chain):
    """
    Sorts a list of dictionaries based on a nested property.

    Args:
    lst: The list of dictionaries to sort.
    property_chain: A list of keys representing the nested property path.

    Returns:
    The sorted list of dictionaries.
    """
    def get_nested_property(obj, property_chain):
        if not property_chain:
            return obj
        return get_nested_property(obj.get(property_chain[0], {}), property_chain[1:])

    return sorted(lst, key=lambda x: get_nested_property(x, property_chain))

filenames = ['monitorings-12290-ec2.json', 'monitorings-12290-k8s.json']
data = []

for filename in filenames:
        with open(filename) as f:
                # Load JSON data from file
                loaded_data = sort_object_list_using_nested_property(json.load(f), ['time', '$date', '$numberLong'])
                data.append(loaded_data)
                # Now data is a Python dictionary

number_of_docs = len(data[0])

good_time_count = 0
bad_time_count = 0
time_threshold_sec = get_value_within_bounds('Enter time threshold (sec):', 5, 500)

good_value_match_count = 0
bad_value_match_count = 0

reading_sensors = ['Speed', 'Satellites', 'Movement', 'BatteryCurrent', 'Battery', 'Ignition', 'GNSS_Status', 'TowDetection', 'ServerTime', 'GSMSignal', 'BatteryVoltage', 'ExternalVoltage']

# max_readings_keys = []
# max_readings = 0

for i in range(number_of_docs):
        obj1 = data[0][i]
        obj2 = data[1][i]
        time_diff = get_time(obj1) - get_time(obj2)

        # readings = data[0][i]['reading'].keys()
        # number_of_readings= len(readings)

        # if number_of_readings > max_readings:
        #         max_readings = number_of_readings
        #         max_readings_keys = readings
        
        time_cond1 = time_diff < timedelta(seconds=time_threshold_sec)
        time_cond2 = time_diff > timedelta(seconds=-time_threshold_sec)

        if time_cond1 and time_cond2:
                good_time_count += 1
        else:
                bad_time_count += 1
        
        for sensor in reading_sensors:
                if ~compare_properties(obj1['reading'], obj2['reading'], sensor):
                        bad_value_match_count +=1
                        break

good_value_match_count = number_of_docs - bad_value_match_count

print('Docs within time threshold', good_time_count)
print('Docs outside time threshold', bad_time_count)
print('Good value match count', good_value_match_count)
print('Bad value match count', bad_value_match_count)