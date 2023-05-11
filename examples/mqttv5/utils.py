import time
import random

import paho.mqtt.client as mqtt
from paho.mqtt.subscribeoptions import SubscribeOptions

class Callbacks:

    def __init__(self):
        self.messages = []
        self.publisheds = []
        self.subscribeds = []
        self.unsubscribeds = []
        self.disconnecteds = []
        self.connecteds = []
        self.conn_failures = []

    def __str__(self):
        return str(self.messages) + str(self.messagedicts) + str(self.publisheds) + \
            str(self.subscribeds) + \
            str(self.unsubscribeds) + str(self.disconnects)

    def clear(self):
        self.__init__()

    def on_connect(self, client, userdata, flags, reasonCode, properties):
        self.connecteds.append({"userdata": userdata, "flags": flags,
                                "reasonCode": reasonCode, "properties": properties})

    def on_connect_fail(self, client, userdata):
        self.conn_failures.append({"userdata": userdata})

    def wait(self, alist, timeout=2):
        interval = 0.2
        total = 0
        while len(alist) == 0 and total < timeout:
            time.sleep(interval)
            total += interval
        return alist.pop(0)

    def wait_connect_fail(self):
        return self.wait(self.conn_failures, timeout=10)

    def wait_connected(self):
        return self.wait(self.connecteds)

    def on_disconnect(self, client, userdata, reasonCode, properties=None):
        self.disconnecteds.append(
            {"reasonCode": reasonCode, "properties": properties})

    def wait_disconnected(self):
        return self.wait(self.disconnecteds)

    def on_message(self, client, userdata, message):
        self.messages.append({"userdata": userdata, "message": message})

    def published(self, client, userdata, msgid):
        self.publisheds.append(msgid)

    def wait_published(self):
        return self.wait(self.publisheds)

    def on_subscribe(self, client, userdata, mid, reasonCodes, properties):
        self.subscribeds.append({"mid": mid, "userdata": userdata,
                                 "properties": properties, "reasonCodes": reasonCodes})

    def wait_subscribed(self):
        return self.wait(self.subscribeds)

    def unsubscribed(self, client, userdata, mid, reasonCodes, properties):
        self.unsubscribeds.append({"mid": mid, "userdata": userdata,
                                   "properties": properties, "reasonCodes": reasonCodes})

    def wait_unsubscribed(self):
        return self.wait(self.unsubscribeds)

    def register(self, client):
        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe
        client.on_publish = self.published
        client.on_unsubscribe = self.unsubscribed
        client.on_message = self.on_message
        client.on_disconnect = self.on_disconnect
        client.on_connect_fail = self.on_connect_fail

def waitfor(queue, depth, limit):
    total = 0
    while len(queue) < depth and total < limit:
        interval = .5
        total += interval
        time.sleep(interval)

def random_clientid():
    return "mqtt-feature-demos-{id}".format(id = random.randint(0, 1000))

def clean_retained_message(host, port, topic = "#"):
    callback = Callbacks()
    client = mqtt.Client("clean retained".encode("utf-8"), protocol = mqtt.MQTTv5)
    client.loop_start()
    callback.register(client)
    client.connect(host = host, port = port)
    response = callback.wait_connected()
    client.subscribe(topic, options = SubscribeOptions(qos = 0))
    response = callback.wait_subscribed()
    time.sleep(1)
    for message in callback.messages:
        client.publish(message["message"].topic, b"", 0, retain = True)
    client.disconnect()
    client.loop_stop()
    time.sleep(.1)
