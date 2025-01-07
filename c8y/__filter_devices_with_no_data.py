from ignore_constants import *

if len(devices_list) != len(source_ids_list):
    raise IndexError(f"Mismatch of device numbers and source id numbers! Device numbers {len(devices_list)}; Source numbers {len(source_ids_list)}")

device_source_pairs_with_data = list(set(device_source_pairs) - set(device_source_pairs_with_no_data))

print(f'Total devices count: {len(devices_list)}')
print(f'Total data-less devices count: {len(device_source_pairs_with_no_data)}')
print(f'Total data-ful devices count: {len(device_source_pairs_with_data)}')
print('-------------------------------------------')
print(f'Device-source pairs with data: {device_source_pairs_with_data}')
