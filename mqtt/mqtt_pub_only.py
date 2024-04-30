import paho.mqtt.client as mqtt
import time
from random import randint
import json
import datetime
import os

def get_current_time_utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

usernames = ['testing-01']
pwords = ['ProbabyMAN767e4!']
urls = ['mqtt-listeners-azure.xelerate.solutions']

ind = 0

while True:
   total_msg_count = int(input('Enter number of messages (5 to 1000): '))
   if total_msg_count in range(5,1001):
      break

# MQTT broker details
broker = urls[ind]
topic = test_topics[ind]
username = usernames[ind]
password = pwords[ind]

# messages_received = []
# logfile = 'logs.txt'
publish_count = 0
sleep_time_mults = [1, 1, 1, 5, 5, 5, 5, 10, 10, 360, 360]

for i in range(total_clients):
   try:
      print(username,'is sending data on topic', topic,'...')

      # Callback function for when the client has connected to the broker
      def on_connect(client, userdata, flags, rc):
         if rc == 0:
            print("Connected to MQTT Broker!")
         else:
            print("Failed to connect, return code %d\n", rc)

      # Callback function for when a message is published
      def on_publish(client, userdata, mid):
         global publish_count
         print(f"{get_current_time_utc()}: Message Published...mid={str(mid)}")
         publish_count += 1

      # def on_message(client, userdata, msg):
      #    global rcvd_count
      #    msg_rec = msg.payload.decode()
      #    print(f"Received message: {msg_rec} on topic: {msg.topic}")
      #    rcvd_count += 1
      #    messages_received.append(msg_rec)
      
      client_id = f'script-test-Pub-{randint(100,999)}'
      client = mqtt.Client(client_id)

      # Assign callback functions
      client.on_connect = on_connect
      client.on_publish = on_publish
      # client.on_message = on_message

      # Set username and password
      client.username_pw_set(username, password)

      # Connect to the broker
      client.connect(broker)

      start = int(input("Enter start: "))
      limit = start + total_msg_count

      print("Start is", str(start))

      while True:
         second_interval = int(input('Enter seconds input (1 to 60): '))
         if second_interval in range(1,61):
            break

      # Start loop to process received messages
      client.subscribe(sub_topic)
      print("Subbed to", sub_topic + '\n')
      client.loop_start()

      for i in range(start, limit): 
         sleep_time = second_interval * sleep_time_mults[randint(0, len(sleep_time_mults))]

         client.publish(topic, payload=json.dumps({"start": start, "msg_num": i ,"client":client_id, "sleep_time_s": sleep_time, "t":get_current_time_utc()}))      
         time.sleep(sleep_time)

      time.sleep(4)
      client.loop_stop()

      # try:
      #    os.remove(logfile)
      # except OSError:
      #    pass

      # with open(logfile, 'x') as file:
      #    for line in messages_received:
      #       file.write(line+'\n')
      
      print(f"""
      Report
      ----------------------------------------
      Index: {ind}
      Interval: {second_interval}
      Pub topic: {topic}
      Username: {username}
      Start: {start}
      Total messages count: {total_msg_count}
      Published message count: {publish_count}  
      ----------------------------------------""")

   except KeyboardInterrupt:
      print('Operation stopped.')
