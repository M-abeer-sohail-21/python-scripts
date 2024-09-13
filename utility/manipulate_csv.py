from pandas import read_csv, json_normalize, to_datetime, notnull
from csv import reader
from json import load
from re import search
from ignore_constants import sensor_id_list
from os import path, remove
from pytz import timezone

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
    v1_v2_name_mapping = generate_dict(mapping_file_path)
    df.insert(insert_location, value1, df[value2].map(v1_v2_name_mapping))

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
base_path = "/home/sarwan/Downloads/"
file_suffixes = ['analytics']
file_prefix_input= 'salesforce'
generate_base_csv = True # TODO: Logic needs fixing
local_timezone = 'Europe/London'
# Edit here STOP -----------------------------------------------------------------------

number_of_files = len(file_suffixes)

for i in range(number_of_files):
    input_path = f'{base_path}/{file_prefix_input}-{file_suffixes[i]}.json'
    output_path = f'{base_path}/{file_prefix_input}-{file_suffixes[i]}.csv'
    temp_path = f'{base_path}/{file_prefix_input}-{file_suffixes[i]}-temp.csv'

    df = None
    temp_file_available = False

    try:
        if path.exists(temp_path):
            df = read_csv(temp_path)
            temp_file_available = True
            generate_base_csv = False
        else:    
            with open(input_path, 'r') as file:
                data = load(file)
                df = json_normalize(data)
    except FileNotFoundError:
        print('[Error] Temp file not found! generating csv from JSON')
        with open(input_path, 'r') as file:
            data = load(file)
            df = json_normalize(data)
    
    if generate_base_csv:
        df.to_csv(temp_path, index=False)

    # Needs fixing
    # if not already_parsed:
    #     with open(input_path, 'r') as file:
    #         data = load(file)
    #         df = json_normalize(data)
    # else:
    #     df = read_csv(output_path)

    # --------------- FOR ABDS BIKES ----------------------------------------------------------------- #
    
    # df['bike_number'] = df['name'].apply(lambda x: extract_substring(x, r'\b(\d{5})\b'))
    # df['updatedAt.unix'] = to_datetime(df['updatedAt.$date.$numberLong'], unit='ms')
    # df['updatedAt'] = df['updatedAt.unix'].apply(lambda x: x.tz_localize('UTC').tz_convert('Asia/Karachi').isoformat())

    # columns_order = ['imei','internalId','bike_number',	'name',	'hub_iccid', 'c8y_iccid', 'status', 'lastMessage']

    # replace_substring_from_columns('packetFromPlatform.c8y_Mobile.','')
    # replace_substring_from_columns('packetFromPlatform.c8y_Availability.','')
    # replace_substring_from_columns('packetFromPlatform.c8y_Hardware.','')
    # replace_substring_from_columns('iccid','c8y_iccid')
    # replace_substring_from_columns('serialNumber','hub_iccid')
    # delete_columns(list(set(df.columns.tolist()) - set(columns_order)))
    # df['status'] = df['status'].replace('UNAVAILABLE', 'DOWN').replace('AVAILABLE','ACTIVE')
    
    # rearrange_columns(columns_order)

    # -------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------------- #

    # --------------- FOR CONN. CBT. COSTA RICA ------------------------------------------------------ #

    # df['time.unix'] = to_datetime(df['time.$date.$numberLong'], unit='ms')
    # df['time'] = df['time.unix'].apply(lambda x: x.tz_localize('UTC').tz_convert('America/Costa_Rica').isoformat())

    # df['createdAt.unix'] = to_datetime(df['createdAt.$date.$numberLong'], unit='ms')
    # df['createdAt'] = df['createdAt.unix'].apply(lambda x: x.tz_localize('UTC').tz_convert('America/Costa_Rica').isoformat())

    # delete_columns(['time.$date.$numberLong','createdAt.$date.$numberLong'])

    # -------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------------- #

    # --------------- Manipulate Salesforce "analytics" -------------------------------------------- #

    print(df.columns())
    print(len(df))
    # df['time.utc'] = to_datetime(df['time.$date.$numberLong'], unit='ms')
    # df['time'] = df['time.utc'].apply(lambda x: x.tz_localize('UTC').tz_convert(local_timezone).isoformat())

    # -------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------------- #

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

    # -------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------------- #

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

    # -------------------------------XXXXXXXXXXXXXXXXXX----------------------------------------------- #

    print(df.head(20))

    if path.exists(output_path):
        remove(output_path)
    df.to_csv(output_path, index=False)