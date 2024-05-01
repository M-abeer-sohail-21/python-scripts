import paho.mqtt.client as mqtt
from datetime import datetime, timezone
from random import randint

def get_current_time_utc():
    return datetime.now(timezone.utc).isoformat()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print(f"{get_current_time_utc()}: Connected with result code {str(rc)}\n")
    for topic in topics:
        client.subscribe(topic)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(f"{get_current_time_utc()}: " + msg.topic + " " + str(msg.payload) + '\n')

# The callback for when the client disconnects from the server.
def on_disconnect(client, userdata, rc):
    if rc == 0:
        print(f"{get_current_time_utc()}: Successfully disconnected from the broker\n")
    else:
        print(f"{get_current_time_utc()}: Unexpected disconnection, result code {str(rc)}\n")

topics = ['testing/topic/+'] # ["/server/acrelHW/#", "/gw/acrelHW/#"]
broker_url = "broker.xelerate.solutions" # mqtt-listeners-azure.xelerate.solutions 
username = "testing-01"
password = "ProbabyMAN767e4!"

client = mqtt.Client(client_id="script-test-Sub-" + str(randint(100, 999)))
client.username_pw_set(username, password)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

client.connect(broker_url, 1883, 300)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
try:
    client.loop_forever()
except KeyboardInterrupt:
    client.disconnect()
    print('Ending sub loop.\n')