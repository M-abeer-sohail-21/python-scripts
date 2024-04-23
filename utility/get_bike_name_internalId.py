from json import load
from pandas import json_normalize

def find_matches(my_string, pattern):

    match = find(pattern, my_string)
    if match:
        return match
    else:
        return None


data_file_path = './utility/down_bikes.json'

with open(data_file_path, 'r') as f:
    data = load(f)

df = json_normalize(data)

df['bike_number'] = df['name'].str.extract(r'\b(\d{5})\b', expand=False)

print(
    list(
        map(
            lambda x:list(df[x]), ['bike_number', 'internalId']
        )
    )
)
