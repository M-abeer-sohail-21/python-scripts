import pandas as pd
import matplotlib.pyplot as plt
from os import path, makedirs
from ignore_constants import *

# NO DATA FOR THESE (as of 2024-05-13): 63593, 14787, 14912, 70079, 72748, 96168, 11865
# DATA WAY FAR BACK (as of 2024-05-15): 50221, 50224, 12483, 98681, 12287

try:
    if len(devices_list) != len(source_ids_list):
        raise ValueError('Device list and source list mismatch!')    

except ValueError as e:
    print('[Error]: ', end=' ')
    print(e)
    exit()

dont_skip = False
devices_with_no_data = []

sources = [(source_ids_list[i], devices_list[i]) for i in range(len(devices_list))]

try:
    number_of_days = int(input('Enter last number of days worth of data to process: '))
    if number_of_days <= 0:
        raise ValueError('Value of number of days must be 1 or more!')
        
except Exception as e:
    print('Error: Exception occurred: ', e)

values_to_plot = [('ExternalVoltage', 'Bike voltage'),('GNSS_Status', 'GSM Status'), ('GSMSignal', 'Signal bars'), ('BatteryVoltage', 'Tracker battery voltage'), ('Battery', 'Tracker battery percent'), ('Satellites', 'Satellite count'), ('UnplugDetection', 'Unplug Alert')]

count = 1
num_of_sources = len(sources)

for source in sources:
    try:
        if not dont_skip:
            result = input('Press enter to continue, type skip to skip this message... ')
            if result == "skip":
                dont_skip = True
        
        internal_id = source[0]
        device_number = source[1]

        flattened_data = pd.read_csv(f'./c8y/meas_csv_results/{device_number}.csv', index_col='server_time', low_memory=False)
        flattened_data.index = pd.to_datetime(flattened_data.index, utc=True, format='ISO8601')

        if flattened_data.empty:
            raise ValueError('Df is empty!')

        duration = flattened_data.index.max() - flattened_data.index.min()

        print(f'duration: {flattened_data.index.max()} to {flattened_data.index.min()}')
        print('Number of days:', duration.days)

        print(flattened_data.head(5))
        print('--------------------------------------------------------------------------------------------------------------------------------')

        for value_to_plot_tuple in values_to_plot:
            try:
                value_to_plot = value_to_plot_tuple[0]
                value_to_plot_name = value_to_plot_tuple[1]
                unit_for_value = flattened_data[f'{value_to_plot}.unit'].iloc[0]            

                print(f'Processing {value_to_plot} for device {device_number}')
                print('*' * 30)

                # Check if the duration is less than two days
                if duration <= pd.Timedelta(days=number_of_days):
                    # If duration is less than two days, use the entire duration for resampling
                    # Assuming 'number_of_days' is the number of days you want to go back from today

                    daily_average = flattened_data[f'{value_to_plot}.value'].resample('Min').mean()
                    
                else:
                    # If duration is more than two days, resample for the last two days
                    # Assuming 'number_of_days' is the number of days you want to go back from today
                    cutoff_date = flattened_data.index.max() - pd.Timedelta(days=number_of_days)

                    # Filter the DataFrame to include only rows where the index (time) is greater than or equal to the cutoff date
                    last_n_days = flattened_data.loc[flattened_data.index >= cutoff_date]

                    daily_average = last_n_days[f'{value_to_plot}.value'].resample('Min').mean()

                # Plotting scatter plot
                plt.scatter(daily_average.index.to_numpy(), daily_average.to_numpy(), marker='x')

                # Set plot title and labels
                plt.title(f'Avg. {value_to_plot_name} over the last {number_of_days} days of activity for {device_number}')
                plt.xlabel('Time')
                plt.ylabel(f'{value_to_plot_name} ({unit_for_value})')
                plt.grid(True) # Enable the grid
                plt.xticks(rotation=45) # Adjust the rotation angle as needed

                output_folder = f'./c8y/plots/{device_number}'

                # Save the plot as a PNG file
                if not path.exists(output_folder):
                    makedirs(output_folder)
                plt.savefig(f'{output_folder}/hourly_average_plot_{device_number}_{value_to_plot_name}.png', dpi=300, bbox_inches='tight')

                # Optionally close the plot window
                plt.close()
            
            except KeyError as e:
                print(f'Ran into an error, key not found: {e}, device number {device_number}')

            except Exception as e:
                print(f'🤣🤣 Ran into an error while plotting data for {device_number}: {e}')
        
        print(f'Processed device {count} of {num_of_sources}')
        count += 1
    except Exception as e:
        print(f'🤣 Exception for {device_number};', e)
        if type(e) == type(ValueError()):
            devices_with_no_data.append(source[1])
    

print(f'devices with no data {list(set(devices_with_no_data))}')
