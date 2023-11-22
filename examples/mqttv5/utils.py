import time
import random
import string

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
        return str(self.messages) + str(self.publisheds) + str(self.publisheds) + \
            str(self.subscribeds) + \
            str(self.unsubscribeds) + str(self.disconnecteds)

    def clear(self):
        self.__init__()

    def on_connect(self, client, userdata, flags, reasonCode, properties):
        self.connecteds.append({"userdata": userdata, "flags": flags,
                                "reasonCode": reasonCode, "properties": properties})

    def on_connect_fail(self, client, userdata):
        self.conn_failures.append({"userdata": userdata})
    
    def wait(self, queue, count = 1, timeout = 2):
        total = 0
        interval = .2
        while len(queue) < count and total < timeout:
            total += interval
            time.sleep(interval)

        q = []
        for i in range(0, len(queue)):
            q.append(queue.pop(0))

        return q

    def wait_connect_fail(self, timeout = 10):
        return self.wait(self.conn_failures, 1, timeout).pop(0)

    def wait_connected(self, timeout = 2):
        return self.wait(self.connecteds, 1, timeout).pop(0)

    def on_disconnect(self, client, userdata, reasonCode, properties=None):
        self.disconnecteds.append(
            {"reasonCode": reasonCode, "properties": properties})

    def wait_disconnected(self, timeout = 2):
        return self.wait(self.disconnecteds, 1, timeout).pop(0)

    def on_message(self, client, userdata, message):
        self.messages.append({"userdata": userdata, "message": message})

    def wait_messages(self, count, timeout = 2):
        return self.wait(self.messages, count, timeout)

    def published(self, client, userdata, msgid):
        self.publisheds.append(msgid)

    def wait_published(self, timeout = 2):
        return self.wait(self.publisheds, 1, timeout)

    def on_subscribe(self, client, userdata, mid, reasonCodes, properties):
        self.subscribeds.append({"mid": mid, "userdata": userdata,
                                 "properties": properties, "reasonCodes": reasonCodes})

    def wait_subscribed(self, timeout = 2):
        return self.wait(self.subscribeds, 1, timeout).pop(0)

    def unsubscribed(self, client, userdata, mid, reasonCodes, properties):
        self.unsubscribeds.append({"mid": mid, "userdata": userdata,
                                   "properties": properties, "reasonCodes": reasonCodes})

    def wait_unsubscribed(self, timeout = 2):
        return self.wait(self.unsubscribeds, 1, timeout).pop(0)

    def register(self, client):
        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe
        client.on_publish = self.published
        client.on_unsubscribe = self.unsubscribed
        client.on_message = self.on_message
        client.on_disconnect = self.on_disconnect
        client.on_connect_fail = self.on_connect_fail

def random_clientid():
    return "mqtt-feature-demos-{id}".format(id = random.randint(0, 1000))

def random_string(length = 8):
    return ''.join(random.sample(string.ascii_letters + string.digits, length))

def clean_retained_message(host, port, topic = "#"):
    callback = Callbacks()
    client = mqtt.Client("clean retained".encode("utf-8"), protocol = mqtt.MQTTv5)
    client.loop_start()
    callback.register(client)
    client.connect(host = host, port = port)
    client.loop_start()
    callback.wait_connected()
    client.subscribe(topic, options = SubscribeOptions(qos = 0))
    callback.wait_subscribed()
    time.sleep(1)
    for message in callback.messages:
        client.publish(message["message"].topic, b"", 0, retain = True)
    client.disconnect()
    client.loop_stop()
    time.sleep(.1)

def in_progress():
    return "\033[1;34m\u2026\033[0m "

def success():
    return "\033[1;32m\u2714\033[0m "

def fail():
    return "\033[1;31m\u2718\033[0m "

def highlight(content):
    if isinstance(content, str):
        content1 = content
    else:
        content1 = str(content)
    return "\033[0;33m" + content1 + "\033[0m"

def print_in_progress(str):
    print(in_progress() + str)

def print_success(str):
    print(success() + str)

def print_fail(str):
    print(fail() + str)