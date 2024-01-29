import asyncio
import asyncio_mqtt as aiomqtt
import json
from datetime import datetime, timezone
from random import randint

sleep_time_mults = [1, 1, 1, 5, 5, 5, 5, 10, 10, 360, 360]

def get_current_time_utc():
   return datetime.now(timezone.utc).isoformat()

async def publish_messages(client, client_id, topic, delay, msg_count, start):
  global sleep_time_mults
  async with client:
   for i in range(start, msg_count + start):
       actual_delay = delay * sleep_time_mults[randint(0,len(sleep_time_mults)-1)]
       message = json.dumps({"start": start,
                             "msg_num": i,
                             "client":client_id,
                             "sleep_time_s": actual_delay,
                             "t":get_current_time_utc()})
     

       await client.publish(topic, payload=message)
       print(f"{get_current_time_utc()}: Published message number {i} for client {client_id} on topic: {topic}")
       await asyncio.sleep(actual_delay)

def get_int_value(input_msg, min, max):
   return_val = 0
   while True:
       return_val = int(input(input_msg))
       if return_val in range(min, max + 1):
           break
       else:
           print(f'Value must be between {min} and {max}')
   return return_val

def get_total_clients():
   return get_int_value('\nEnter total number of clients: ', 1, 10)

def get_total_msg_count():
   return get_int_value('Enter total msg count per client: ', 2, 10)

async def main():
   try:
       broker = "mqtt-listeners.xelerate.solutions"
       base_topic = "testing/topic/"

       total_clients = get_total_clients()
       msg_count = get_total_msg_count()
       delay = get_int_value('Enter base delay value', 5, 60)

       client_ids = [f"script-test-Pub-{str(x).zfill(2)}" for x in range(total_clients)]
       topics = [f"{base_topic}{str(x).zfill(2)}" for x in range(total_clients)]

       username = "delete-me-01"
       password = "ProbabyMAN767e4!"       

       tasks = []       

       for i in range(total_clients):
           start = get_int_value(f'Enter a starting value for {client_ids[i]}: ', 0, 99999999)
           client = aiomqtt.Client(broker, client_id=client_ids[i], username=username, password=password)
           task = publish_messages(client, client_ids[i], topics[i], delay, msg_count, start)
           tasks.append(task)

       await asyncio.gather(*tasks)

   except KeyboardInterrupt:
       print('Bye bye...')

asyncio.run(main())
