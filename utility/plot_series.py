import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv('/home/sarwan/Downloads/monitorings.csv')
df['time'] = pd.to_datetime(df['time'], format='ISO8601')

# Assuming df is already loaded from your CSV file
# Verify the column names
print("Column names:", df.columns)

name_id_map = {
    871661917: 'CR Ingeniería B2B - Congelador',
    261965114:'CR Centro de Experiencia B2B'
}

# Check if 'sensorId' exists in the DataFrame, otherwise use the correct column name
sensor_id_column = 'sensorId'
if sensor_id_column not in df.columns:
    print(f"Column '{sensor_id_column}' not found. Available columns are {list(df.columns)}.")
else:
    # Proceed with filtering and plotting
    filtered_df = df.loc[(df[sensor_id_column] == 261965114) | (df[sensor_id_column] == 871661917), ['sensorId','time', 'reading.Temperature.value']]

    print(filtered_df.head(10))
    
    plt.figure(figsize=(10, 6))
    for sensor_id, group in filtered_df.groupby(sensor_id_column):
        plt.plot(np.array(group['time']), np.array(group['reading.Temperature.value']), label=f'{name_id_map[sensor_id]}', alpha=(sensor_id == 261965114) * 0.9 + (sensor_id == 871661917) * 0.4, marker='x')
    
    plt.grid(True, color="grey", linewidth=1.4, linestyle="-.")
    plt.xlabel('Time')
    plt.ylabel('Temperature (\u00b0 C)')
    plt.title('Temperature vs Time')
    plt.legend()
    plt.show()
