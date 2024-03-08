import pandas as pd
import matplotlib.pyplot as plt
from os import path, makedirs

# Needs updating -> Error in line 27

# Edit here START ------------
sources = [('3456', '70091'), ('46964', '12485'), ('77532', '50224'), ('2636415', '12053'), ('87107442643', '63593')]
values_to_plot = [('ExternalVoltage', 'Bike voltage'),('GNSS_Status', 'GSM Status'), ('GSMSignal', 'Signal bars'), ('BatteryVoltage', 'Tracker battery voltage'), ('Battery', 'Tracker battery percent'), ('Satellites', 'Sattelite count')]
# Edit here STOP -------------

for source in sources:
    internal_id = source[0]
    bike_number = source[1]

    flattened_data = pd.read_csv(f'meas_csv_results/{bike_number}.csv', index_col='time', low_memory=False)
    flattened_data.index = pd.to_datetime(flattened_data.index, utc=True, format='ISO8601')

    print('\nBike', bike_number)
    print(flattened_data.head(2))
    print('--------------------------------------------------------------------------------------------------------------------------------')

    for value_to_plot_tuple in values_to_plot:
        value_to_plot = value_to_plot_tuple[0]
        value_to_plot_name = value_to_plot_tuple[1]
        unit_for_value = flattened_data[f'{value_to_plot}.unit'].iloc[0]

        duration = flattened_data.index.max() - flattened_data.index.min()

        # Check if the duration is less than two days
        if duration <= pd.Timedelta(days=2):
            # If duration is less than two days, use the entire duration for resampling
            daily_average = flattened_data[f'{value_to_plot}.value'].resample('h').mean()
        else:
            # If duration is more than two days, resample for the last two days
            last_two_days = flattened_data.last('2D')
            daily_average = last_two_days[f'{value_to_plot}.value'].resample('h').mean()

        # Plotting scatter plot
        plt.scatter(daily_average.index.to_numpy(), daily_average.to_numpy(), marker='x')

        # plt.plot(flattened_data.index.to_numpy(), flattened_data[f'{value_to_plot}.value'].to_numpy())

        # Set plot title and labels
        plt.title(f'Avg. hourly {value_to_plot_name} Over Last Two days of measurements for {bike_number}')
        plt.xlabel('Time')
        plt.ylabel(f'{value_to_plot_name} ({unit_for_value})')
        plt.grid(True) # Enable the grid
        plt.xticks(rotation=45) # Adjust the rotation angle as needed

        output_folder = f'plots/{bike_number}'

        # Save the plot as a PNG file
        if not path.exists(output_folder):
            makedirs(output_folder)
        plt.savefig(f'{output_folder}/hourly_average_plot_{bike_number}_{value_to_plot_name}.png', dpi=300, bbox_inches='tight')

        # Optionally close the plot window
        plt.close()