from os import listdir, path, makedirs
from re import search
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from collections import Counter
from json import loads

# ------------------------------------------------------------------- Edit START -------------------------------------------------------------------

plot_folder = "./device_plots"
base_logs_path = './raw_logs'
file_prefix = 'Explore-logs-'
substrings = ["[Global] Info	", " Teltonika TCP:"]
pattern = r'Teltonika TCP: Packet for device (\d{15})'
time_parse_pattern = "%m/%d/%Y, %I:%M:%S %p"
json_str_start = '{"AVL_Data":['
json_str_end = ' - {}'

# ------------------------------------------------------------------- Edit STOP --------------------------------------------------------------------

if base_logs_path[-1] != '/':
    base_logs_path = base_logs_path + '/'

makedirs(plot_folder, exist_ok=True)

def list_files_with_prefix(directory, prefix):
    files = listdir(directory)
    matching_files = [f'{base_logs_path}{file}' for file in files if file.startswith(prefix)]
    
    return matching_files

def round_down_to_nearest_n(n, dt):
    return dt - timedelta(minutes=dt.minute % n, seconds=dt.second, microseconds=dt.microsecond)

def return_time_imei_pair(pattern, time_start_index, time_stop_index, line):
        
        time_str = line[time_start_index:time_stop_index]
        
        imei_match = search(pattern, line)
        
        if imei_match:
            imei = imei_match.group(1)
            return (time_str, imei)

file_list = list_files_with_prefix(base_logs_path, file_prefix)

main_dict = {}

for _i in range(len(file_list)):
    
    print("Processing file:", file_list[_i])
    with open(file_list[_i], 'r') as f:
        lines = f.read().split('\n')
        
        for i in range(len(lines)):
            line = lines[i]
            
            if substrings[0] in line:
                time_start_index = line.index(substrings[0]) + len(substrings[0])
                time_stop_index = line.index(substrings[1])            
            
                time_imei_pair = return_time_imei_pair(pattern, time_start_index, time_stop_index, line)

                json_obj = loads(line[line.index(json_str_start):line.index(json_str_end)])

                data_obj_count = len(json_obj['AVL_Data'])
                
                if time_imei_pair != None:
                    dict_key = time_imei_pair[1]
                    dict_list_val = time_imei_pair[0]
                    if dict_key not in main_dict.keys():
                        main_dict[dict_key] = []
                    for i in range(data_obj_count):
                        main_dict[dict_key].append(dict_list_val)
                

for item in main_dict.keys():
    main_dict[item] = map(lambda x: datetime.strptime(x, time_parse_pattern), main_dict[item])

for device_id, timestamps in main_dict.items():    
    rounded_timestamps = [round_down_to_nearest_n(5,dt) for dt in timestamps]    
    timestamp_counts = Counter(rounded_timestamps)    
    sorted_timestamps = sorted(timestamp_counts.items(), key=lambda x: x[0])
    
    cumulative_sum = [0]
    for _, count in sorted_timestamps:
        cumulative_sum.append(cumulative_sum[-1] + count)
    cumulative_sum.pop(0)
    
    times = [item[0] for item in sorted_timestamps]
    
    plt.figure(figsize=(10, 5))
    plt.plot(times, cumulative_sum, marker = 'x')
    plt.title(f'Cumulative Measurement Obj Counts for IMEI {device_id} (Binned in 5-Minute Intervals)')
    plt.xlabel('Time')
    plt.ylabel('Cumulative Count')
    plt.xticks(rotation=45)
    
    if times:
        min_time = min(times) - timedelta(minutes=15)  # 15 minutes before the smallest timestamp
        max_time = max(times) + timedelta(minutes=15)  # 15 minutes after the largest timestamp
        plt.xlim(min_time, max_time)
    
    plt.tight_layout()
    plt.grid(axis='y') 
    
    plot_file = path.join(plot_folder, f"{device_id}.png")
    plt.savefig(plot_file)
    plt.close() 
