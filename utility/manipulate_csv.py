from pandas import read_csv, json_normalize, to_datetime, notnull
from csv import reader
from json import load
from re import search
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

def remap_values(v1, v2, mapping_file_path, insert_index):
    global df
    v1_v2_name_mapping = generate_dict(mapping_file_path)
    df.insert(insert_index, v1, df[v2].map(v1_v2_name_mapping))

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

# Edit here START ----------------------------------------------------------------------
base_path = '/home/sarwan/Downloads'
number_of_files = 1
# Edit here STOP -----------------------------------------------------------------------

for i in range(1, number_of_files + 1):
    input_path = f'{base_path}/abds-all-bikes.json' # -{temp_suffix[i - 1]}
    output_path = f'{base_path}/abds-all-bikes.csv'

    with open(input_path, 'r') as file:
        data = load(file)

    df = json_normalize(data)
    
    df['bike_number'] = df['name'].apply(lambda x: extract_substring(x, r'\b(\d{5})\b'))
    df['updatedAt.unix'] = to_datetime(df['updatedAt.$date.$numberLong'], unit='ms')
    df['updatedAt'] = df['updatedAt.unix'].apply(lambda x: x.tz_localize('UTC').tz_convert('Asia/Karachi').isoformat())

    columns_order = ['bike_number', 'internalId','name','imei','iccid', 'status', 'lastMessage']

    replace_substring_from_columns('packetFromPlatform.c8y_Mobile.','')
    replace_substring_from_columns('packetFromPlatform.c8y_Availability.','')
    replace_substring_from_columns('packetFromPlatform.c8y_Hardware.','')
    replace_substring_from_columns('iccid','incomplete_iccid')
    replace_substring_from_columns('serialNumber','iccid')
    delete_columns(list(set(df.columns.tolist()) - set(columns_order)))
    df['status'] = df['status'].replace('UNAVAILABLE', 'DOWN').replace('AVAILABLE','ACTIVE')
    
    rearrange_columns(columns_order)

    # -----------------------------------------------------------------------
    # Get data from Teltonika parser output

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

    # -----------------------------------------------------------------------

    print(df.head(20))

    df.to_csv(output_path, index=False)
    
