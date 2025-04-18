from dotenv import load_dotenv
from os import getenv
from requests import get
from json import loads
import pandas as pd

def replace_substring_from_columns(df, substring, to_replace_with):
    df.columns = df.columns.str.replace(substring, to_replace_with)

BASE_URL = "https://albusayra.emea.cumulocity.com/"

alarms_url = f'{BASE_URL}/alarm/alarms'

status_labels = ['ACTIVE', 'ACKNOWLEDGED']

alarms_columns_to_keep = ['id', 'type', 'text', 'status', 'severity', 'count', 'time', 'creationTime', 'lastUpdated', 'firstOccurrenceTime', 'dataPoint', 'locations', 'ruleId', 'generatedBy','source.id', 'source.name']

device_ids_for_verification = []

load_dotenv()

alarms_list = []

auth_token = getenv('C8Y_ABDS_TOKEN')
headers = {
                'User-Agent': 'my-app/0.0.1',
                'Accept': 'application/json',
                'Authorization': auth_token,
            }

for status in status_labels:
    page_count = 1
    while True:        
        query_params = {'status':status, "pageSize": 1800, 'currentPage':page_count}

        print(f'Making request at {alarms_url} with status {status} currentPage {page_count}')

        query_response = get(alarms_url, params=query_params, headers=headers)
        payload = loads(query_response.text)

        alarms_data = payload["alarms"]

        if len(alarms_data) == 0:            
            print("Empty alarms list at page count", page_count)
            break
        else:
            page_count += 1
            alarms_list.extend(alarms_data)

print('Size of alarms list', len(alarms_list))

alarms_df = pd.json_normalize(alarms_list)
alarm_columns_to_remove = list(set(alarms_df.columns.to_list()) - set(alarms_columns_to_keep))
alarms_df.drop(columns=alarm_columns_to_remove, inplace=True)

replace_substring_from_columns(alarms_df, 'source.id', 'device_id')
replace_substring_from_columns(alarms_df, 'source.name', 'device_name')
replace_substring_from_columns(alarms_df, 'status', 'alarm_status')
replace_substring_from_columns(alarms_df, 'text', 'alarm_text')
replace_substring_from_columns(alarms_df, 'type', 'alarm_type')
replace_substring_from_columns(alarms_df, 'creationTime', 'creation_time')
replace_substring_from_columns(alarms_df, 'firstOccurrenceTime', 'first_occurrence_time')
replace_substring_from_columns(alarms_df, 'lastUpdated', 'update_time')

alarms_df['alarm_text'] = alarms_df['alarm_text'].map(lambda value: value.encode('utf-8').hex())

alarms_df.to_csv('alarms_backup_c8y.csv')

print(alarms_df.head())