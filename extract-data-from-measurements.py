import pandas as pd
import matplotlib.pyplot as plt
import json

# Edit here START ------------
sources = [("34695733","14787"), ("49237155","97405"), ("2483", "98681")]
values_to_plot = ['ExternalVoltage','GNSS_Status', 'GSMSignal', 'BatteryVoltage', 'Battery', 'Satellites']
# Edit here STOP -------------

for source in sources:
    internal_id = source[0]
    bike_number = source[1]
    all_data = []
    api_request_page_count = 1
    meas_json_file = f'meas_json_results/{internal_id}.json'

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

    print(internal_id)
    print(bike_number)
    print(flattened_data.head(3), flattened_data.tail(3), sep = '\n')
    print('--------------------------------------------------------------------------------------------------------------------------------')

    for value_to_plot in values_to_plot:
        unit_for_value = flattened_data[f'{value_to_plot}.unit'].iloc[0]

        # Calculate the daily average of value_to_plot
        daily_average = flattened_data[f'{value_to_plot}.value'].resample('h').mean()

        # Plotting scatter plot
        plt.scatter(daily_average.index.to_numpy(), daily_average.to_numpy(), marker='x')

        # plt.plot(flattened_data.index.to_numpy(), flattened_data[f'{value_to_plot}.value'].to_numpy())

        # Set plot title and labels
        plt.title(f'Avg. hourly {value_to_plot} Over Time for {bike_number}')
        plt.xlabel('Time')
        plt.ylabel(f'{value_to_plot} ({unit_for_value})')
        plt.grid(True) # Enable the grid
        plt.xticks(rotation=45) # Adjust the rotation angle as needed

        # Save the plot as a PNG file
        plt.savefig(f'plots/{bike_number}/hourly_average_plot_{bike_number}_{value_to_plot}.png', dpi=300, bbox_inches='tight')

        # Optionally close the plot window
        plt.close()