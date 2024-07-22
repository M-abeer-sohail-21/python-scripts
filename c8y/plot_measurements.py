import pandas as pd
import matplotlib.pyplot as plt
from os import path, makedirs

# NO DATA FOR THESE (as of 2024-05-13): 63593, 14787, 14912, 70079, 72748, 96168, 11865
# DATA WAY FAR BACK (as of 2024-05-15): 50221, 50224, 12483, 98681, 12287

# TODO: Modify to get device list and source id list from file

devices_list = [96201, 96168, 98681, 12692, 12278, 12270, 12485, 12483, 12482, 12687, 98253, 12273, 10622, 12108, 10617, 50224, 12495, 11865, 10614, 10616, 12276, 10372, 50221, 12084, 12693, 14773, 14779, 14913, 14787, 72743, 63593, 72748, 14912, 10619, 10620, 12609, 12486, 12490, 63589, 12694, 72759, 72763, 96199, 97405, 12245, 12075, 12269, 12279, 97295, 98637]

source_ids_list = [389, 3420, 2483, 2508, 3456, 3482, 46964, 47674, 47718, 49492, 61683, 63055, 69952, 69305, 74249, 77532, 152454, 388751, 1049556, 1053103, 1068248, 1128729, 1422168, 2636415, 48727, 6253904087, 3953908687, 29139236, 34695733, 31114007536, 87107442643, 60107407843, 26637896, 70048, 73289, 66930, 2530, 393621, 47107417697, 3467, 51107442290, 37107398393, 3241, 49237155, 66127, 49386, 337216, 3464, 3416, 4853912316]

auth_token = ''

devices_with_no_data = []

try:
    # Edit here START ------------
    devices_of_interest = [97295, 98637, 10619]
    # Edit here STOP -------------

    sources_to_make = [devices_list.index(x) for x in devices_of_interest]

except ValueError as e:
    print('[Error]: Device number inputted that is not there!', end=' ')
    print(e)
    exit()

dont_skip = False

sources = [(source_ids_list[i], devices_list[i]) for i in sources_to_make]

number_of_days = int(input('Enter number of days worth of data to process: '))

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
                print(f'Ran into an error, key not found: {e}')

            except Exception as e:
                print(f'🤣🤣 Ran into an error while plotting data for {device_number}: {e}')
                devices_with_no_data.append(source[1])
        
        print(f'Processed device {count} of {num_of_sources}')
        count += 1
    except Exception as e:
        print('🤣', e)
        devices_with_no_data.append(source[1])
    

print(f'devices with no data {list(set(devices_with_no_data))}')
