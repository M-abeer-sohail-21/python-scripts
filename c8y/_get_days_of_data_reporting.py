import pandas as pd
from datetime import timedelta, datetime
from ignore_constants import *

# NO DATA FOR THESE (as of 2024-05-13): 63593, 14787, 14912, 70079, 72748, 96168
# DATA WAY FAR BACK (as of 2024-05-15): 50221, 50224, 12483, 98681, 12287

def get_days_of_data_reporting(device_number):
    global report_contents

    file_to_load = f'./c8y/meas_csv_results/{device_number[0]}-{device_number[1]}.csv'

    data = pd.read_csv(file_to_load, index_col='server_time',low_memory=False)
    data.index = pd.to_datetime(data.index, utc=True, format='ISO8601')

    unique_dates = pd.Series(data.index).map(lambda x: x.date()).unique()
    number_of_unique_dates = len(unique_dates)

    sorted_dates = sorted(unique_dates)

    strftime_pattern = "%Y-%m-%d"

    # Initialize variables for the start of the current range
    date_ranges = []

    start_date_local_range = None
    end_date_local_range = None

    # Iterate through the sorted dates
    for i in range(number_of_unique_dates - 1):
        
        current_date = sorted_dates[i]
        next_date_in_range = sorted_dates[i+1]

        if i == 0:
            start_date_local_range = current_date
        
        if next_date_in_range != current_date + timedelta(days=1):
            end_date_local_range = current_date
            date_ranges.append(f"{start_date_local_range.strftime(strftime_pattern)} to {end_date_local_range.strftime(strftime_pattern)}")

            start_date_local_range = next_date_in_range

            if i == number_of_unique_dates - 2:
                end_date_local_range = start_date_local_range
                date_ranges.append(f"{start_date_local_range.strftime(strftime_pattern)} to {end_date_local_range.strftime(strftime_pattern)}")
        else:
            if i == number_of_unique_dates - 2:
                end_date_local_range = next_date_in_range
                date_ranges.append(f"{start_date_local_range.strftime(strftime_pattern)} to {end_date_local_range.strftime(strftime_pattern)}")

    report_contents += f'First date in range: {sorted_dates[0]}\n'
    
    for range_str in date_ranges:
        report_contents += range_str + '\n'
    
    report_contents += f'Last date in range: {sorted_dates[-1]}\n\n'

report_contents = ''

report_contents += 'Days of data reporting:\n'
report_contents += '************************\n\n'

for device in device_source_pairs_with_data:
    try:
        report_contents += f'Device number: {device[0]}-{device[1]}\n'
        report_contents += '-----------------------------------\n'
        get_days_of_data_reporting(device)
    except Exception as e:
        report_contents += f'🤣 Error: {e}\n'

report_contents += 'End of report\n*************'

with open('.' + ''.join(''.join(f'/c8y/days_of_data_reporting/{datetime.now()}'.split(':')).split('.')) + '.txt','x') as report_file:
    report_file.write(report_contents)