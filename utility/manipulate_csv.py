from pandas import read_csv, json_normalize, to_datetime, notnull
from csv import reader
from json import load

def delete_columns(col):
    global df
    df.drop(columns=col, inplace=True)

def rearrange_columns(order):
    global df
    df = df[order]

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

def replace_substring_from_columns(substring, to_replace):
    global df
    df.columns = df.columns.str.replace(substring, to_replace)

def move_column_to_position(column, pos):
    global df
    # Step 1: Remove the column
    column_to_move = df.pop(column)

    # Step 2: Insert the column at the desired position
    df.insert(pos, column_to_move.name, column_to_move)

number_of_files = 1
sensor_id_name_mapping = generate_dict('/home/sarwan/Downloads/salesforce-devices-name.csv')
print(sensor_id_name_mapping)

for i in range(1, number_of_files + 1):
    input_path = '/home/sarwan/Downloads/salesforce-monitorings-dump.json'
    output_path = '/home/sarwan/Downloads/salesforce-monitorings-data-final.csv'

    with open(input_path, 'r') as file:
        data = load(file)

    df = json_normalize(data)

    df.insert(1, 'name', df['sensorId'].map(sensor_id_name_mapping))

    convert_epoch_to_iso('time.$date.$numberLong', 'ms')

    delete_columns(['tenant', 'time.$date.$numberLong'])

    replace_substring_from_columns('reading.', '')

    move_column_to_position('time', 3)

    print(df.head(20))

    df.to_csv(output_path, index=False)
    
