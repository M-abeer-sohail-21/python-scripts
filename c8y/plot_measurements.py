import pandas as pd
import matplotlib.pyplot as plt
from os import path, makedirs

devices_list = [63589, 70091, 63593, 12053, 12485, 12483, 70079, 17018, 14912, 
              17020, 50221, 96168, 98681, 14787, 97405, 12108, 72748, 50224, 
              70080, 96199, 14913, 17019, 70089, 12287, 72763, 10616, 12286, 70086, 50241, 12115, 10620, 10622, 10617, 12495, 14779, 72759]
source_ids_list = [47107417697, 3456, 87107442643, 2636415, 46964, 47674, 1068248, 2002381, 26637896,
                   439804, 1422168, 3420, 2483, 34695733, 49237155, 69305, 60107407843, 77532,
                   3482, 3241, 29139236, 434557, 2508, 61683, 37107398393, 1053103, 388751, 3467, 63055, 66930, 73289, 69952, 74249, 152454, 3953908687, 51107442290]

# No data from 01 Jan onwards: 12483 - 96168
# Online but danger: 63589, 97405, 14913
# No data: 72748 (Did not connect after installation)
# No data on c8y: 12483, 70079, 17018, 14912, 17020, 96168

sources_to_make = []

try:
    # Edit here START ------------
    devices_of_interest = [10617, 10620, 10622, 12115, 12495, 14779, 50241, 72759]
    # Edit here STOP -------------
    sources_to_make = [devices_list.index(x) for x in devices_of_interest]

except ValueError as e:
    print('[Error]: Device number inputted that is not there!', end=' ')
    print(e)
    exit()

sources = [(source_ids_list[i], devices_list[i]) for i in sources_to_make]

number_of_days = int(input('Enter number of days worth of data to process: '))

values_to_plot = [('ExternalVoltage', 'Bike voltage'),('GNSS_Status', 'GSM Status'), ('GSMSignal', 'Signal bars'), ('BatteryVoltage', 'Tracker battery voltage'), ('Battery', 'Tracker battery percent'), ('Satellites', 'Satellite count'), ('UnplugDetection', 'Unplug Alert')]

for source in sources:
    internal_id = source[0]
    device_number = source[1]

    flattened_data = pd.read_csv(f'./c8y/meas_csv_results/{device_number}.csv', index_col='time', low_memory=False)
    flattened_data.index = pd.to_datetime(flattened_data.index, utc=True, format='ISO8601')

    print('\nDevice', device_number)
    print(flattened_data.head(5))
    print('--------------------------------------------------------------------------------------------------------------------------------')

    for value_to_plot_tuple in values_to_plot:
        try:
            value_to_plot = value_to_plot_tuple[0]
            value_to_plot_name = value_to_plot_tuple[1]
            unit_for_value = flattened_data[f'{value_to_plot}.unit'].iloc[0]

            print(f'duration = {flattened_data.index.max()} - {flattened_data.index.min()}')

            duration = flattened_data.index.max() - flattened_data.index.min()

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

            print(f'\nDuration for {value_to_plot}:', str(duration.days) + '. Number of days:', number_of_days)
            print('HEAD data\n------------')
            print(flattened_data.last(f'{number_of_days}D')[f'{value_to_plot}.value'].head(10))
            print('TAIL data\n------------')
            print(flattened_data.last(f'{number_of_days}D')[f'{value_to_plot}.value'].tail(10))
            print('*' * 30)

            # Plotting scatter plot
            plt.scatter(daily_average.index.to_numpy(), daily_average.to_numpy(), marker='x')

            # plt.plot(flattened_data.index.to_numpy(), flattened_data[f'{value_to_plot}.value'].to_numpy())

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
            print(f'Ran into an error while plotting data for {device_number}: {e}')