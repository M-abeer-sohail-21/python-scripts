from datetime import datetime, timedelta
from pytz import timezone
from random import choice, random, randint
from dateutil import parser
from json import dumps, dump

# ---------------- EDIT START ----------------------------------------------------------------------------------------------------------------------- #
tenant = ""
hourly_interval = 1/12 # Number of datapoints per hour. 1 means only 24 readings in a day
days_for_monitorings = 1 # Number of days worth of data for monitorings
days_for_aggregations = 1 # Number of day worth of data for aggregations
start_date_monitorings = '2024-05-10T00:00:00.000Z' # format must be like '2024-05-10T00:00:00.000Z'
start_date_aggregations = '2023-07-29T00:00:00.000Z' # format must be like '2024-05-10T00:00:00.000Z'
time_zone_location = "Asia/Karachi" # Choose from TC Identifier column here: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List
# ---------------- EDIT STOP ------------------------------------------------------------------------------------------------------------------------ #

if 'Z' not in start_date_aggregations or 'Z' not in start_date_monitorings:
    raise ValueError('[ERROR] Must specify time in correct format (Check the comment near the edit part)')

datetime_parse_format = "%Y-%m-%dT%H:%M:%S.%fZ"

data_type_points_map = {"data": ['PM2_5', 'Temperature', 'UVIntensity', 'Cubic_Humidity', 'CO2', 'BatteryVoltage', 'FilterExpireError', 'UVError', 'Cubic_VOC', 'PM1', 'PM4', 'Humidity', 'AQI', 'Cubic_HCHO', 'Cubic_Temp', 'IsCentigrade', 'SignalStrength', 'UV', 'CO2_Temp', 'FilterRemainingTime', 'Temperature_F', 'UVRunTime', 'pollutant_source', 'Fan', 'PM10', 'Cubic_TVOC', 'DeviceCurrent', 'ExternalVoltage'], "ping": ["SignalStrength"]}

datapoint_metadatas = {
	"PM2_5":{
		"unit": "µg/m³",
		"value": (0,10,0)
	},
	"Temperature": {
		"unit": "°C",
		"value": (20,30,3)
	},
		"UVIntensity": {
		"value": (0,5,1)
	},
	"Cubic_Humidity": {
		"unit": "%",
		"value": (20,50,1)
	},
	"CO2": {
		"unit": "ppm",
		"value": (200,1000,0)
	},
	"BatteryVoltage": {
		"unit": "V",
		"value": (3,4,5)
	},
	"FilterExpireError": {
		"unit": "state",
		"value": (0,2,0)
	},
	"UVError": {
		"unit": "state",
		"value": (0,2,0)
	},
	"Cubic_VOC": {
		"unit": "ppb",
		"value": (100,150,0)
	},
	"PM1": {
		"unit": "µg/m³",
		"value": (0,1,1)
	},
	"PM4": {
		"unit": "µg/m³",
		"value": (0,1,1)
	},
	"Humidity": {
		"unit": "%",
		"value": (20,80,4)
	},
	"AQI": {
		"unit": "Index",
		"value": (20,50,0)
	},
	"Cubic_HCHO": {
		"unit": "ppb",
		"value": (0,10,0)
	},
	"Cubic_Temp": {
		"unit": "°C",
		"value": (20,30,0)
	},
	"IsCentigrade": {
		"unit": "state",
		"value": (0,1,0)
	},
	"SignalStrength": {
		"unit": "bars",
		"value": (0,5,0)
	},
	"UV": {
		"unit": "state",
		"value": (0,1,0)
	},
	"CO2_Temp": {
		"unit": "°C",
		"value": (15,20,3)
	},
	"FilterRemainingTime": {
		"unit": "hours",
		"value": (3999,8999,0)
	},
	"Temperature_F": {
		"unit": "°F",
		"value": (68,86,4)
	},
	"UVRunTime": {
		"unit": "hours",
		"value": (3999,8999,0)
	},
	"pollutant_source": {
		"value": (0,3,0)
	},
	"Fan": {
		"unit": "speed",
		"value": (0,3,0)
	},
	"PM10": {
		"unit": "µg/m³",
		"value": (0,3,0)
	},
	"Cubic_TVOC": {
		"unit": "ppb",
		"value": (40,60,0)
	},
	"DeviceCurrent": {
		"unit": "A",
		"value": (0,1,6)
	},
	"ExternalVoltage": {
		"unit": "V",
		"value": (3,5,5)
	}
}

def generate_monitorings_data(datapoint_names):

    global datapoint_metadatas

    meas_data_obj = {}
    for datapoint in datapoint_names:
        metadata_tuple = datapoint_metadatas[datapoint]["value"]
        min_datapoint = metadata_tuple[0]
        max_datapoint = metadata_tuple[1]
        number_dps = metadata_tuple[2]

        if number_dps != 0:
            generated_data = round(random() * (max_datapoint - min_datapoint) + min_datapoint, number_dps)
        else:
            generated_data = int(random() * (max_datapoint - min_datapoint) + min_datapoint)

        meas_data_obj[datapoint] = {"value": generated_data}

        if "unit" in datapoint_metadatas[datapoint].keys():
            meas_data_obj[datapoint]['unit'] = datapoint_metadatas[datapoint]['unit']
        
    return meas_data_obj

def generate_aggregations_data(datapoint, mode, data_obj_datetime, device_id):

    global datapoint_metadatas, tenant

    data_obj_timestamp = str(int(data_obj_datetime.timestamp() * 1000))
    
    metadata_tuple = datapoint_metadatas[datapoint]['value']
    min_metdata_value = metadata_tuple[0]
    max_metdata_value = metadata_tuple[1]

    mean_value = (max_metdata_value + min_metdata_value) / (random() * 5)
    random_offset = mean_value * random()
    min_value = mean_value - random_offset
    max_value = mean_value + random_offset

    no_of_readings = int((mode == "daily") * (randint(100,200)) + (mode == "hourly") * (randint(20,50)))

    sum_of_readings = no_of_readings * randint(20,30)

    sum_of_square = no_of_readings * randint(55,66)

    data_obj = {
                "time":{
                    "$date": {
                        "$numberLong": 0
                    }
                },
                "sensorId": device_id,
                "mode": mode,
                "tenant": tenant,
                "parameterName":datapoint,
                "createdAt": {
                    "$date": {
                        "$numberLong": 0
                    }
                }, "updatedAt":{
                    "$date": {
                        "$numberLong": 0
                    }
                },
                "max": max_value,
                "min": min_value,
                "mean": mean_value,
                'noOfReadings': no_of_readings,
                'sumOfReadings': sum_of_readings,
                'sumOfSquare': sum_of_square,
                'stDiv': mean_value * randint(1,2) / 100
            }
    
    if "unit" in datapoint_metadatas[datapoint].keys():
        data_obj["unit"] = datapoint_metadatas[datapoint]['unit']

    data_obj['time']['$date']['$numberLong'] = data_obj_timestamp
    data_obj['createdAt']['$date']['$numberLong'] = data_obj_timestamp
    data_obj['updatedAt']['$date']['$numberLong'] = data_obj_timestamp
    
    return data_obj

def create_test_monitorings_data(device_id_list):

    now = datetime.strptime(start_date_monitorings, datetime_parse_format).astimezone(tz=timezone(time_zone_location))

    final_data_obj = []

    for device_id in device_id_list:
        for day in range(days_for_monitorings):
            for hour in range(int(24 * hourly_interval)):
                
                data_type = choice(['data','data','data','data','ping'])

                readings_obj = generate_monitorings_data(data_type_points_map[data_type])
                
                data_obj_datetime = now + timedelta(days=day) + timedelta(hours=hour)

                print("[MONITORINGS]: Making data for device id", device_id, "timestamp:", data_obj_datetime.isoformat())

                data_obj_timestamp = str(int(data_obj_datetime.timestamp() * 1000))

                data_obj = {
                        "time":{
                            "$date": {
                                "$numberLong": data_obj_timestamp
                            }
                        }, "sensorId": device_id,
                        "type": data_type,
                        "tenant": tenant,
                        "reading": readings_obj,
                        "createdAt": {
                            "$date": {
                                "$numberLong": data_obj_timestamp
                            }
                        }, "updatedAt":{
                            "$date": {
                                "$numberLong": data_obj_timestamp
                            }
                        }
                    }
                
                final_data_obj.append(data_obj)
    
    return final_data_obj

def create_test_analytics_data(device_id_list):
    now = datetime.strptime(start_date_aggregations, datetime_parse_format).astimezone(tz=timezone(time_zone_location))
    now = now.replace(minute=0, second=0)

    final_data_obj = []

    for device_id in device_id_list:

        for day in range(days_for_monitorings):

            data_obj_datetime = now + timedelta(days=day)

            for hour in range(24):
                
                data_obj_datetime = now + timedelta(days=day) + timedelta(hours=hour)

                print("[AGGREGATIONS]: Making data for device id", device_id, "and mode hourly for timestamp", data_obj_datetime.isoformat())

                for datapoint in data_type_points_map['data']:

                    data_obj = generate_aggregations_data(datapoint, "hourly", data_obj_datetime, device_id)
                    
                    final_data_obj.append(data_obj)

            print("[AGGREGATIONS]: Making data for device id", device_id, "and mode daily for timestamp", data_obj_datetime.isoformat())

            for datapoint in data_type_points_map['data']:

                data_obj = generate_aggregations_data(datapoint, "daily", data_obj_datetime, device_id)

                final_data_obj.append(data_obj)
    
    return final_data_obj

def write_data_to_json_file(final_data, output_file_path):
    with open(output_file_path, 'w') as f:
        dump(final_data, f)

# Test output
# write_data_to_json_file(create_test_analytics_data(['123','456']), './test_a_output.json')
# write_data_to_json_file(create_test_monitorings_data(['123', '456']), './test_m_output.json')