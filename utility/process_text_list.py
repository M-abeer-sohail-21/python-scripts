from re import findall
import pandas as pd

all_records = []

patterns = [r'\b\d{12,22}\b', r'\b\d{5}\b', r'\d+$']
device_specific_number = 'bike_number'

base_path = '/home/sarwan/Downloads/temp'
file_path = 'emails-to-process/email-list.txt'
identifier_substring = 'Xelerate Rule'

def find_matches(my_string, pattern):

    match = findall(pattern, my_string)
    if match:
        return match
    else:
        return None

with open(f'{base_path}/{file_path}', 'r') as f:
    text_list = f.read().split('\n')[:-1]

for line in text_list:

    if identifier_substring not in line:
        continue

    split_names = line.split(' - ')

    alarm_name = split_names[1]
    device_name = split_names[3]

    identifiers = list(map(lambda y,x=device_name: find_matches(x, y), patterns))

    temp_dict = {}

    if identifiers[0] != None and identifiers[1] != None:
        imei, *others = identifiers[0]
        bike_num, *others = identifiers[1]
        temp_dict = {
            "imei": imei,
            device_specific_number: bike_num
        }
    elif identifiers[2] != None:
        imei, *others = identifiers[2]
        temp_dict = {
            "imei": imei
        }
    elif all(element is None for element in identifiers):
        raise ValueError('All nones in the identifiers list!')
    
    temp_dict['alarm'] = alarm_name
    temp_dict['time'] = split_names[-1].split('.')[0][:10]

    all_records.append(temp_dict)

df = pd.DataFrame(all_records)
df.to_csv(f'{base_path}/abds_alarms_csv.csv', index=False)