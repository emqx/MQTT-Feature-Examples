import json
import utils

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

HOST = "broker.emqx.io"
PORT = 1883

class Callbacks2(utils.Callbacks):
    def on_message(self, client, userdata, message):
        if hasattr(message.properties, 'SubscriptionIdentifier'):
            for identifier in message.properties.SubscriptionIdentifier:
                match message.properties.SubscriptionIdentifier[0]:
                    case 1:
                        self.__log(message)
                    case 2:
                        self.__check_pm25(message)
        self.messages.append({"userdata": userdata, "message": message})

    def __log(self, message):
        print("[Log] Receive message (Topic = %s, Payload = %s)" % (message.topic, message.payload.decode('utf-8')))

    def __check_pm25(self, message):
        if json.loads(message.payload)['pm2.5'] > 50:
            print("[Air purifier] Turn on and shift to first gear...")

def publish(client, topic, payload):
    print("[Publish] Topic: %s, Payload: %s" % (topic, payload))
    client.publish(topic, payload)

callback = Callbacks2()

clientid = utils.random_clientid()
client = mqtt.Client(clientid.encode("utf-8"), protocol = mqtt.MQTTv5)
callback.register(client)

client.connect(host = HOST, port = PORT, clean_start = True)
client.loop_start()
response = callback.wait_connected()

sub_properties = Properties(PacketTypes.SUBSCRIBE)
sub_properties.SubscriptionIdentifier = 1
client.subscribe(clientid + "/home/+", qos = 2, properties = sub_properties)
response = callback.wait_subscribed()

sub_properties.SubscriptionIdentifier = 2
client.subscribe(clientid + "/home/PM2_5", qos = 2, properties = sub_properties)
response = callback.wait_subscribed()

publish(client, clientid + "/home/PM2_5", json.dumps({'pm2.5': 60}))
utils.waitfor(callback.messages, 2, 2)

print("\n")
publish(client, clientid + "/home/temperature", json.dumps({'temperature': 27.3}))
utils.waitfor(callback.messages, 3, 2)

client.disconnect()
callback.wait_disconnected()
client.loop_stop()
