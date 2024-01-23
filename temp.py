import re
import json
from dateutil import parser
from datetime import datetime, timedelta

logfile = 'logs.txt'
jsonfile = 'ProdK8S.json'

json_objs_paho = []
json_objs_mqttx = []
json_data = None
message_count_tally = True
mismatch_instances = []
bad_count = 0

count = 1

with open(logfile, 'r') as file:
  for line in file:
      match = re.search(r'(\{.+\})', line)
      if match:
          print('JSON object found, count', count)
          json_str = match.group(1)
          json_obj = json.loads(json_str)
          json_obj['createAt'] = parser.parse(json_obj['t'])
          json_objs_paho.append(json_obj)
          count+=1

print('\nPaho message count:', count)

with open('ProdK8S.json', 'r') as file:
   json_data = json.load(file)

count = 1

for item in json_data['messages']:
  payload = json.loads(item['payload'])
  payload['createAt'] = datetime.strptime(item['createAt'], "%Y-%m-%d %H:%M:%S:%f")
  json_objs_mqttx.append(payload)
  print("MQTTX JSON obj Count:", count)
  count += 1

print("\nMessages count MQTTX:", len(json_data['messages']))

paho_count = len(json_objs_paho)
mqttx_count = len(json_objs_mqttx)

if (paho_count != mqttx_count):
  print('WARNING: Messages count does not tally!')
  message_count_tally = False

msg_count = max(paho_count, mqttx_count)

json_objs_paho = sorted(json_objs_paho, key=lambda d: d['id'])
json_objs_mqttx = sorted(json_objs_mqttx, key=lambda d: d['id'])

time_diff_threshold_s = int(float(input('Enter time diff threshold in s: ')))

for i in range(msg_count):
  current_paho = json_objs_paho[i]
  current_mqttx = json_objs_mqttx[i]
  time_diff = current_paho['createAt'] - current_mqttx['createAt']
  if time_diff > timedelta(seconds=time_diff_threshold_s) or time_diff < timedelta(seconds=-time_diff_threshold_s):
    bad_count += 1
  if current_mqttx['id'] != current_paho['id']:
    mismatch_instances.append({'paho':current_paho['id'], "mqttx":current_mqttx['id']})

print(f"""
  Report
  ------------------------------------
  Total message count: {msg_count}
  Message count tallied: {message_count_tally}
  Mismatch instances: {len(mismatch_instances)}
  Very late messages: {bad_count}
  Time diff threshold (s): {time_diff_threshold_s}
  ------------------------------------
  Mismatch instances (none if mismatch count is 0)""")

for mismatch in mismatch_instances:
  print(mismatch)

print('------------------------------------\nEnd of report.')