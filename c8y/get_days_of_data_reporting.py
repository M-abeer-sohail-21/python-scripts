import pandas as pd

# Make it better

device_number = '17019'
file_to_load = f'./c8y/meas_csv_results/{device_number}.csv'

data = pd.read_csv(file_to_load, index_col='time', low_memory=False)
data.index = pd.to_datetime(data.index, utc=True, format='ISO8601')

unique_dates = pd.Series(data.index).map(lambda x: x.date()).unique()

sorted_dates = sorted(unique_dates)

# Initialize variables for the start of the current range
start_date = None
date_ranges = []

# Iterate through the sorted dates
for date in sorted_dates:
    if start_date is None or date.day != start_date.day + 1:
        # If this is the start of a new range, add the previous range to the list
        if start_date is not None:
            date_ranges.append(f"{start_date.strftime('%Y-%m-%d')} to {date.strftime('%Y-%m-%d')}")
        # Update the start of the current range
        start_date = date

# Add the last range to the list
if start_date is not None:
    date_ranges.append(f"{start_date.strftime('%Y-%m-%d')} to {date.strftime('%Y-%m-%d')}")

# Print the date ranges
for range_str in date_ranges:
    print(range_str)