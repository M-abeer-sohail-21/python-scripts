from pandas import read_csv, json_normalize, to_datetime, notnull
from csv import reader
from json import load

def delete_columns(col):
    global df
    df.drop(columns=col, inplace=True)

def rearrange_columns(order, delete_others=False):
    global df
    print(Warning("All column names must be unqiue! Expect weird behavior otherwise."))
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

def convert_epoch_to_iso(column_name, unit='s'):
    global df
    # Convert epoch time to datetime, handling errors by coercing invalid values
    df['time'] = to_datetime(df[column_name], unit=unit, errors='coerce')
    # Convert datetime to ISO format
    df['time'] = df['time'].apply(lambda x: x.isoformat() if notnull(x) else None)

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
        

base_path = '/home/sarwan/Downloads'

number_of_files = 2

temp_prefix = ['c8y', 'bridge']

for i in range(1, number_of_files + 1):
    input_path = f'{base_path}/temp-abds-{temp_prefix[i - 1]}.json'
    output_path = f'{base_path}/abds-{temp_prefix[i - 1]}-offline.csv'

    with open(input_path, 'r') as file:
        data = load(file)

    df = json_normalize(data)

    # rearrange_columns(["name", "internalId", "deviceType", "tenant", "packetFromPlatform.c8y_Hardware.serialNumber", "packetFromPlatform.c8y_Mobile.cellId", "packetFromPlatform.c8y_Mobile.iccid", "packetFromPlatform.c8y_Mobile.imei", "packetFromPlatform.c8y_Mobile.imsi", "packetFromPlatform.c8y_Mobile.lac", "packetFromPlatform.c8y_Mobile.mcc", "packetFromPlatform.c8y_Mobile.mnc"])

    # replace_substring_from_columns("packetFromPlatform.", "")
    # replace_substring_from_columns("c8y_Mobile.", "")
    # replace_substring_from_columns("c8y_Hardware.", "")

    # condition = df['iccid'].notnull() & df['iccid'].ne('')

    # print(condition)

    # filter_columns(["name", "iccid"], condition, True).to_csv(f'{base_path}/result.csv', index=False)

    # remap_values(v1, v2, mapping_file_path, 1)

    # convert_epoch_to_iso('time.$date.$numberLong', 'ms')

    # delete_columns(['tenant', 'time.$date.$numberLong'])

    # replace_substring_from_columns('reading.', '')

    # move_column_to_position('time', 3)

    print(df.head(20))

    df.to_csv(output_path, index=False)
    
