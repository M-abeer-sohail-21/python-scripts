import pandas as pd
import matplotlib.pyplot as plt
import json

# Edit here START ------------
source = "34695733" 
values_to_plot = ['ExternalVoltage','GNSS_Status', 'GSMSignal', 'BatteryVoltage', 'Battery', 'Satellites']
# Edit here STOP -------------

all_data = []
api_request_page_count = 1
meas_json_file = f'meas_json_results/{source}.json'

try:
    with open(meas_json_file, 'r') as file:
        all_data = json.load(file)
except FileNotFoundError as e:
    print('Error accessing measurements file:', e, 'exiting...')
    exit()

# Flatten the nested dictionaries within the 'payload' key
flattened_data = pd.json_normalize([item['payload'] for item in all_data])

# Add the 'time' column to the flattened DataFrame
flattened_data['time'] = [item['time'] for item in all_data]

# Convert 'time' column to datetime
flattened_data['time'] = pd.to_datetime(flattened_data['time'])
flattened_data.set_index('time', inplace=True)
flattened_data.sort_index(inplace=True)

for value_to_plot in values_to_plot:
    unit_for_value = flattened_data[f'{value_to_plot}.unit'].iloc[0]

    # Calculate the daily average of value_to_plot
    daily_average = flattened_data[f'{value_to_plot}.value'].resample('h').mean()

    # Plotting scatter plot
    plt.scatter(daily_average.index.to_numpy(), daily_average.to_numpy(), marker='x')

    # plt.plot(flattened_data.index.to_numpy(), flattened_data[f'{value_to_plot}.value'].to_numpy())

    # Set plot title and labels
    plt.title(f'Avg. hourly {value_to_plot} Over Time')
    plt.xlabel('Time')
    plt.ylabel(f'{value_to_plot} ({unit_for_value})')
    plt.grid(True) # Enable the grid

    # Save the plot as a PNG file
    plt.savefig(f'plots/hourly_average_plot_{value_to_plot}.png', dpi=300, bbox_inches='tight')

    # Optionally close the plot window
    plt.close()