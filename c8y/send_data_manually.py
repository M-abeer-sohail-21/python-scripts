from os import listdir
from json import loads, dumps, load
import jsonpath_ng

folder_path = 'abds-manual-data-logs' # Edit here
device_ids = """866907053430095
866907055235765
866907055234024
866907055150931
866907052467015
866907052435962
866907055126493
866907055235518""".split() # Edit here
search_string = ", context: {\"AVL_Data\":" # Edit here
json_obj_start_index_offset = 11 # Edit here
json_obj_end_index = -2 # Edit here; enter None to include last char 

device_id_folder_map = {}

extracted_json_objs = []

with open('parser.json', 'r') as file:
    global parser
    parser = load(file)


for device_id in device_ids:
    device_id_folder_map[device_id] = f"{folder_path}/{device_id[-4:]}"

for device_id in device_ids:
    extracted_json_objs.append({"device_id": device_id, "payloads": []})
    for file_name in listdir(device_id_folder_map[device_id]):
        if ".summary.log" not in file_name:
            file_path = f"{device_id_folder_map[device_id]}/{file_name}"
            with open(file_path, 'r') as file:
                matching_lines = [line for line in file if search_string in line]
                if matching_lines:
                    for line in matching_lines:
                        json_str = line[line.find(search_string) + len(search_string) - json_obj_start_index_offset - 1: json_obj_end_index]
                        json_obj = loads(json_str)
                        extracted_json_objs[-1]["payloads"].append(json_obj)


test_obj = extracted_json_objs[0]
                    