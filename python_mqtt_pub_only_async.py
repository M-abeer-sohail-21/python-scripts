import asyncio
import asyncio_mqtt as aiomqtt
import json
from datetime import datetime, timezone

def get_current_time_utc():
   return datetime.now(timezone.utc).isoformat()

async def publish_messages(client, client_id, topic, delay, msg_count):
   async with client:
       start = get_int_value(f'Enter a starting value for {client_id}: ', 0, 10000)

       for i in range(start, msg_count + start):
           message = json.dumps({"start": start,
                                 "msg_num": i,
                                 "client":client_id,
                                 "sleep_time_s": delay,
                                 "t":get_current_time_utc()})

           await client.publish(topic, payload=message)
           print(f'{get_current_time_utc()}: Published message number {i} for client {client_id}')
           await asyncio.sleep(delay)

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

       client_ids = [f"script-test-Pub-{str(x).zfill(2)}" for x in range(total_clients)]
       topics = [f"{base_topic}{str(x).zfill(2)}" for x in range(total_clients)]

       username = "delete-me-01"
       password = "12345abcde"

       msg_count = get_total_msg_count()

       tasks = []

       delay = 3

       for i in range(total_clients):
           client = aiomqtt.Client(broker, client_id=client_ids[i], username=username, password=password)
           task = publish_messages(client, client_ids[i], topics[i], delay, msg_count)
           tasks.append(task)

       await asyncio.gather(*tasks)

   except KeyboardInterrupt:
       print('Bye bye...')

asyncio.run(main())
