import json
import time
import utils

from urllib import parse as urlparse
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

HOST = "broker.emqx.io"
PORT = 1883

def publish(client, topic, payload, properties):
    print("[Publish] Topic: %s, Payload: %s" % (topic, payload))
    client.publish(topic, payload, properties = properties)

def parse(msg):
    if msg.properties.ContentType == "application/json":
        return json.loads(msg.payload)
    elif msg.properties.ContentType == "application/x-www-form-urlencoded":
        params = urlparse.parse_qs(msg.payload)
        return {k.decode('utf-8'): v[0].decode('utf-8') for k, v in params.items()}
    
def timestamp():
    return str(int(time.time() * 1000))

a_callback = utils.Callbacks()
b_callback = utils.Callbacks()

# a_client as publisher
a_clientid = utils.random_clientid()
a_client = mqtt.Client(a_clientid.encode("utf-8"), protocol = mqtt.MQTTv5)
a_callback.register(a_client)

# b_client as subsriber
b_clientid = utils.random_clientid()
b_client = mqtt.Client(b_clientid.encode("utf-8"), protocol = mqtt.MQTTv5)
b_callback.register(b_client)

a_client.connect(host = HOST, port = PORT, clean_start = True)
a_client.loop_start()
response = a_callback.wait_connected()

b_client.connect(host = HOST, port = PORT, clean_start = True)
b_client.loop_start()
response = b_callback.wait_connected()

topic = b_clientid + "/demo"
b_client.subscribe(topic, qos = 2)
response = b_callback.wait_subscribed()

publish_properties = Properties(PacketTypes.PUBLISH)
publish_properties.ContentType = "application/json"
publish_properties.PayloadFormatIndicator = 1
publish(a_client, topic, json.dumps({'msg': 'hello', 'ts': timestamp()}), publish_properties)

publish_properties = Properties(PacketTypes.PUBLISH)
publish_properties.ContentType = "application/x-www-form-urlencoded"
publish_properties.PayloadFormatIndicator = 1
publish(a_client, topic, "msg=hello&ts=" + timestamp(), publish_properties)

# Wait for 2 messages, up to 1 second
messages = b_callback.wait_messages(2, 1)

if len(messages) == 2:
    for message in messages:
        msg = message["message"]
        content = parse(msg)
        print("[Received] Topic: %s, Content Type: %s, Payload Format Indicator: %d, Msg: %s, Timestamp: %s" %
          (msg.topic, msg.properties.ContentType, msg.properties.PayloadFormatIndicator, content['msg'], content['ts']))
else:
    print("Unexpected result")

# Disconnect
a_client.disconnect()
a_callback.wait_disconnected()
a_client.loop_stop()

b_client.disconnect()
b_callback.wait_disconnected()
b_client.loop_stop()
