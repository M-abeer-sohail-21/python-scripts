import re
from datetime import datetime, timedelta

def get_lines_from_file(file_path):
    with open(file_path, 'r') as file:
       lines = file.readlines()
    return lines

def extract_time_and_device_id_data(filename, offset, time_format):
   if time_format == 'MM/DD/YYYY, HH:MM:SS AM/PM':
       pattern = r'(\d{2}/\d{2}/\d{4},\s\d{2}:\d{2}:\d{2}\s(AM|PM)).*?(\d{15})'
   elif time_format == 'YYYY-MM-DD HH:MM:SS.ssssss':
       pattern = r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{6}).*?(\d{15})'
   else:
       print("Invalid time format.")
       return

   result = []
   digit_group = 0
   with open(filename, 'r') as file:
       for line in file:
           match = re.search(pattern, line)
           if match:
               time_string = match.group(1)
               # Convert the time string to a datetime object
               if time_format == 'MM/DD/YYYY, HH:MM:SS AM/PM':
                  time = datetime.strptime(time_string, "%m/%d/%Y, %I:%M:%S %p")
                  digit_group = 3
               elif time_format == 'YYYY-MM-DD HH:MM:SS.ssssss':
                  time = datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S.%f")
                  digit_group = 2
               # Add the offset to the time
               time += timedelta(hours=offset)
               # Convert the time back to a string and save it in the dictionary
               time_string = time.strftime("%Y-%m-%d %H:%M:%S.%f")
               data = {'time': time_string, 'digit': match.group(digit_group)}
               print(data)
               result.append(data)
   # Sort the result by increasing time
   result = sorted(result, key=lambda x: datetime.strptime(x['time'], "%Y-%m-%d %H:%M:%S.%f"))
   return result

file_paths = ['orano_sub_logs.txt', 'orano_sub_logs_hub.txt']
time_formats = ['YYYY-MM-DD HH:MM:SS.ssssss', 'MM/DD/YYYY, HH:MM:SS AM/PM']
offsets = [-5,0]

results = []

for i in range(2):
    result = extract_time_and_device_id_data(file_paths[i], offsets[i], time_formats[i])
    print(len(result))
    print('\n')
    results.append(result)

for i in range(len(results[0])):
    print(i+1)
    device_id= results[0][i]['digit']
    if results[0][i]['digit'] == results[1][i]['digit']:
        print('ID match!', device_id)
    else:
        print('ID mismatch!', device_id)
    time_diff = datetime.strptime(results[0][i]['time'], "%Y-%m-%d %H:%M:%S.%f") - datetime.strptime(results[1][i]['time'], "%Y-%m-%d %H:%M:%S.%f")
    print('big time diff:', time_diff > timedelta(seconds=3) or time_diff < timedelta(seconds=-500))
    print('\n')

