from pandas import read_csv, json_normalize, to_datetime, notnull, DataFrame, read_json
from csv import reader
from json import load
from re import search
from ignore_constants import sensor_id_list
from os import path, remove
from pytz import timezone
from ijson import parse
from datasets import load_dataset
from datetime import datetime

def delete_columns(col):
    global df
    df.drop(columns=col, inplace=True)

def rearrange_columns(order, delete_others=False):
    global df
    print(Warning("All column names must be unique! Expect weird behavior otherwise."))
    df = df[order]

    if delete_others:
        all_cols = list(df.columns)
        cols_to_delete = list(set(all_cols) - set(order))
        delete_columns(cols_to_delete)

def generate_dict(input_file_path, int_key = False):
    with open(input_file_path, 'r') as file:
        data = reader(file)
        # Skip the header row if your CSV has one
        next(data)

        my_dict = {}
        
        if int_key == True:
            # Create a dictionary from the CSV data
            my_dict = {int(rows[0]): rows[1] for rows in data}
        else:
            my_dict = {rows[0]: rows[1] for rows in data}
        
        return my_dict

def convert_epoch_to_iso(column_name, new_column_name, unit='s'):
    global df
    # Convert epoch time to datetime, handling errors by coercing invalid values
    df[new_column_name] = to_datetime(df[column_name], unit=unit, errors='coerce')
    # Convert datetime to ISO format
    df[new_column_name] = df[new_column_name].apply(lambda x: x.tz_localize('UTC').tz_convert('Asia/Karachi').isoformat() if notnull(x) else None)

def replace_substring_from_columns(substring, to_replace_with):
    global df
    df.columns = df.columns.str.replace(substring, to_replace_with)

def move_column_to_position(column, pos):
    global df
    # Step 1: Remove the column
    column_to_move = df.pop(column)

    # Step 2: Insert the column at the desired position
    df.insert(pos, column_to_move.name, column_to_move)

def remap_values(value1, value2, mapping_file_path, insert_location):
    global df
    value1_value2_name_mapping = generate_dict(mapping_file_path)
    df.insert(insert_location, value1, df[value2].map(value1_value2_name_mapping))

def filter_columns(col_names, condition=None, print_output=False):
    global df
    result = df.loc[condition, col_names]
    
    if print_output:
        print(result)
    
    return result
        
def extract_substring(text, pattern):
    match = search(pattern, text)
    if match:
        return match.group(1)  # Assuming you want the first group; adjust the group number as needed
    else:
        return None

def filter_by_column(column, list_of_vals, return_not = False):
    # Check if sensorId column exists
    if column not in df.columns:
        raise ValueError(f"Column '{column}' does not exist in the DataFrame")

    # Create a mask for rows where sensorId is not in the list
    if return_not:
        mask = ~df[column].isin(list_of_vals)
    else:
        mask = df[column].isin(list_of_vals)

    # Apply the mask to filter the DataFrame
    filtered_df = df[mask]

    # Return the length of the filtered result
    return filtered_df

# Edit here START ----------------------------------------------------------------------
BASE_PATH = "/home/sarwan/Downloads" # work/scratchpad/python-scripts"
FILE_SUFFIXES =  ['01']
INPUT_FILE_PREFIX=  'devices'
GENERATE_BASE_CSV = False # TODO: Logic needs fixing
LARGE_DATASET = False # TODO: Logic needs fixing, ALWAYS SET TO FALSE
USE_TEMP_PATH = False
LOCAL_TIMEZONE = 'Asia/Karachi'
# Edit here STOP -----------------------------------------------------------------------

while LARGE_DATASET:
    to_continue = input("Did you account for a large dataset? (y/n) ")
    if to_continue in ["y","n"]:
        if to_continue == "y":
            break
        else:
            exit()

number_of_files = len(FILE_SUFFIXES)

for i in range(number_of_files):

    INPUT_PATH = f'{BASE_PATH}/{INPUT_FILE_PREFIX}-{FILE_SUFFIXES[i]}.json'
    OUTPUT_PATH = f'{BASE_PATH}/{INPUT_FILE_PREFIX}-{FILE_SUFFIXES[i]}.csv'
    TEMP_PATH = f'{BASE_PATH}/{INPUT_FILE_PREFIX}-{FILE_SUFFIXES[i]}-temp.csv'

    df = None
    temp_file_available = False

    try:
        if path.exists(TEMP_PATH) and USE_TEMP_PATH:
            df = read_csv(TEMP_PATH)
            temp_file_available = True
            GENERATE_BASE_CSV = False

    except FileNotFoundError:
        print('[Error] Temp file not found! generating csv from JSON')
        if not LARGE_DATASET:
            if GENERATE_BASE_CSV:
                with open(INPUT_PATH, 'r') as file:
                    data = load(file)
                    df = json_normalize(data)
                    if str(type(df)) != "<class 'NoneType'>":
                        df.to_csv(TEMP_PATH, index=False)

    if LARGE_DATASET:
        data = parse(open(INPUT_PATH, 'r'))
        df = DataFrame()

        # TODO: Maybe this library won't work
        # data = load_dataset("json", data_files=INPUT_PATH) # , split="train")

    else:
        with open(INPUT_PATH, 'r') as file:
            data = load(file)
            df = json_normalize(data)
    
    if str(type(df)) != "<class 'NoneType'>":
        # --------------- FOR ABDS BIKES ----------------------------------------------------------------- #
        
        df['bike_number'] = df['name'].apply(lambda x: extract_substring(x, r'\b(\d{5})\b'))
        # df['updatedAt.unix'] = to_datetime(df['updatedAt.$date.$numberLong'], unit='ms')
        # df['updatedAt'] = df['updatedAt.unix'].apply(lambda x: x.tz_localize('UTC').tz_convert(LOCAL_TIMEZONE).isoformat())

        columns_order = ['imei','internalId','bike_number',	'name',	'hub_iccid', 'c8y_iccid', 'status', 'lastMessage']

        replace_substring_from_columns('packetFromPlatform.c8y_Mobile.','')
        replace_substring_from_columns('packetFromPlatform.c8y_Availability.','')
        replace_substring_from_columns('packetFromPlatform.c8y_Hardware.','')
        replace_substring_from_columns('iccid','c8y_iccid')
        replace_substring_from_columns('serialNumber','hub_iccid')
        delete_columns(list(set(df.columns.tolist()) - set(columns_order)))
        df['status'] = df['status'].replace('UNAVAILABLE', 'DOWN').replace('AVAILABLE','ACTIVE')
        df['lastMessage'] = df['lastMessage'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ"))
        df['lastMessage'] = df['lastMessage'].apply(lambda x: x.tz_localize('UTC').tz_convert(LOCAL_TIMEZONE).isoformat())
        
        rearrange_columns(columns_order)

        # -------------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------- #

        # -------------------------------- Check JM overheating device ----------------------------------- #



        # -------------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------- #
        
        # --------------- PARSING SALESFORCE ANALYTICS (LARGE) ------------------------------------------- #

        # if not LARGE_DATASET:
        #     raise Exception("LARGE_DATASET not set to True!")
        
        # columns = []
        # record_dict = {}
        # records = []

        # count = 0

        # for prefix, event, value in data:
        #     if prefix == "item" and event == "end_map":
        #         records.append(record_dict)
        #         record_dict = {}
        #         count += 1
        #         print(f'Processed records: {count}')
            
        #     if prefix == "item.updatedAt.$date.$numberLong" or prefix == "item.time.$date.$numberLong":
        #         key_name = prefix.split(".")[1]
        #         record_dict[key_name] = value

        #     if "item." in prefix and "_map" not in event: # Ignoring start_map and end_map events
        #         prefix_parts = prefix.split(".")
        #         levels = len(prefix_parts) - 2

        #         if levels == 0:
        #             column_name = prefix_parts[-1]
        #             if column_name not in columns:
        #                 columns.append(column_name)
        #             record_dict[column_name] = value
        
        # df = DataFrame.from_dict(records, orient='columns')

        # for column_name in ['time', 'updatedAt']:
        #     df[column_name] = df[column_name].apply(lambda x: int(x))
        #     df[f'{column_name}_utc'] = to_datetime(df[column_name], unit='ms')
        #     df[column_name] = df[f'{column_name}_utc'].apply(lambda x: x.tz_localize('UTC').tz_convert(LOCAL_TIMEZONE).isoformat())

        # remap_values('name', 'sensorId', f'{BASE_PATH}/salesforce-devices.csv',0)
        # delete_columns(['__v',"_id",'createdAt','sensorId','tenant','time_utc','updatedAt_utc'])        

        # -------------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------- #

        # --------------- FOR CONN. CBT. COSTA RICA ------------------------------------------------------ #

        # df['time.unix'] = to_datetime(df['time.$date.$numberLong'], unit='ms')
        # df['time'] = df['time.unix'].apply(lambda x: x.tz_localize('UTC').tz_convert('America/Costa_Rica').isoformat())

        # df['createdAt.unix'] = to_datetime(df['createdAt.$date.$numberLong'], unit='ms')
        # df['createdAt'] = df['createdAt.unix'].apply(lambda x: x.tz_localize('UTC').tz_convert('America/Costa_Rica').isoformat())

        # delete_columns(['time.$date.$numberLong','createdAt.$date.$numberLong'])

        # -------------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------- #

        # --------------- Manipulate Salesforce "analytics" ---------------------------------------------- #

        # -------------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------- #

        # --------------- Manipulate Salesforce "monitorings" -------------------------------------------- #
        
        # df['time.utc'] = to_datetime(df['time.$date.$numberLong'], unit='ms')
        # df['time'] = df['time.utc'].apply(lambda x: x.tz_localize('UTC').tz_convert(local_timezone).isoformat())

        # if temp_file_available:
        #     sensor_id_list = list(map(lambda x: int(x), sensor_id_list))

        # delete_columns(['__v', '_id.$oid', 'createdAt.$date.$numberLong','tenant','updatedAt.$date.$numberLong','time.$date.$numberLong'])
        # replace_substring_from_columns('reading','')
        # print(f"Length of rows not in sensorId list: {len(filter_by_column('sensorId', sensor_id_list, True))}")
        
        # try:
        #     names_file_path = f'{base_path}/salesforce-devices.csv'
        #     remap_values('name', 'sensorId', names_file_path, 0)
        # except FileNotFoundError:
        #     print('[Error] Names file not found!')

        # -------------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------- #

        # --------------- Get data from Teltonika parser output ------------------------------------------ #

        # final_data = []

        # for elem in data:
        #     for elem2 in elem['data']['Content']['AVL_Datas']:
        #         elem2['ServerTimeStamp'] = elem['timestamp']
        #         elem2['Date'] = elem['date']
            
        #     final_data.extend(elem['data']['Content']['AVL_Datas'])

        # replace_substring_from_columns('GPSelement.','')
        # replace_substring_from_columns('IOelement.Elements.','')
        # replace_substring_from_columns('Timestamp','DeviceTimeStamp')

        # rearrange_columns(['Date','ServerTimeStamp', 'DeviceTimeStamp', 'Priority', 'Longitude', 'Latitude', 'Altitude', 'Angle', 'Satellites', 'Speed', '11', '14', '21', '24', '66', '67', '68', '69', '80', '113', '200', '237', '239', '240', '246', '247', 'IOelement.EventID', 'IOelement.ElementCount'])

        # -------------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------- #

        print(df.head(20))

        if path.exists(OUTPUT_PATH):
            remove(OUTPUT_PATH)
        df.to_csv(OUTPUT_PATH, index=False)