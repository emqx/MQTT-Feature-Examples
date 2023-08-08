import utils
import time

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

HOST = "broker.emqx.io"
PORT = 1883

def publish(client, topic, payload, properties):
    print("[Publish] Topic: %s, Payload: %s" % (topic, payload))
    client.publish(topic, payload, properties = properties)

callback = utils.Callbacks()
callback2 = utils.Callbacks()

# aclient as publisher
aclientid = utils.random_clientid()
aclient = mqtt.Client(aclientid.encode("utf-8"), protocol = mqtt.MQTTv5)
callback.register(aclient)

# bclient as subsriber
bclientid = utils.random_clientid()
connect_properties = Properties(PacketTypes.CONNECT)
connect_properties.SessionExpiryInterval = 300
bclient = mqtt.Client(bclientid.encode("utf-8"), protocol = mqtt.MQTTv5)
callback2.register(bclient)

aclient.connect(host = HOST, port = PORT, clean_start = True)
aclient.loop_start()
response = callback.wait_connected()

bclient.connect(host = HOST, port = PORT, clean_start = True, properties = connect_properties)
bclient.loop_start()
response = callback2.wait_connected()

topic = bclientid + "/demo"
bclient.subscribe(topic, qos = 2)
response = callback2.wait_subscribed()
bclient.disconnect()
callback2.wait_disconnected()
bclient.loop_stop()
callback2.clear()

# Publish messages with the expiry interval after the subscriber goes offline
publish_properties = Properties(PacketTypes.PUBLISH)
publish_properties.MessageExpiryInterval = 5
publish(aclient, topic, "Expiry Interval is 5 seconds", publish_properties)

publish_properties.MessageExpiryInterval = 60
publish(aclient, topic, "Expiry Interval is 60 seconds", publish_properties)

# The subscriber waits 6 seconds before reconnecting
time.sleep(6)
bclient = mqtt.Client(bclientid.encode("utf-8"), protocol = mqtt.MQTTv5)
callback2.register(bclient)

connect_properties = Properties(PacketTypes.CONNECT)
connect_properties.SessionExpiryInterval = 0
bclient.connect(host = HOST, port = PORT, clean_start = False, properties = connect_properties)
bclient.loop_start()
response = callback2.wait_connected()

utils.waitfor(callback2.messages, 2, 1)

if len(callback2.messages) == 1:
    msg = callback2.messages[0]["message"]
    print("[Received] Topic: %s, Payload: %s, Remaining expiry interval: %d" %
          (msg.topic, msg.payload.decode("utf-8"), msg.properties.MessageExpiryInterval))
else:
    print("Unexpected result")

# Disconnect
aclient.disconnect()
callback.wait_disconnected()
aclient.loop_stop()

bclient.disconnect()
callback2.wait_disconnected()
bclient.loop_stop()