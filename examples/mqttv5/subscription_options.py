import time
import utils

import paho.mqtt.client as mqtt
from paho.mqtt.subscribeoptions import SubscribeOptions

HOST = "broker.emqx.io"
PORT = 1883
CLIENTID = utils.random_clientid()
TOPIC = CLIENTID + "/demo"

def publish(client, topic, payload, qos = 0, retain = False):
    print("[Publish] Topic: %s, Payload: %s, QoS: %d, Retain: %d" % (topic, payload, qos, retain))
    client.publish(topic, payload, qos, retain)

def qos():
    # Subscription Options - QoS
    print("Subscription Options - QoS")
    print("--------------------------")

    callback = utils.Callbacks()
    client = mqtt.Client(CLIENTID.encode("utf-8"), protocol = mqtt.MQTTv5)
    callback.register(client)
    client.connect(host = HOST, port = PORT, clean_start = True)
    client.loop_start()
    response = callback.wait_connected()

    client.subscribe(TOPIC, options = SubscribeOptions(qos = 0))
    response = callback.wait_subscribed()
    print("[Subscribe] QoS: 0")

    publish(client, TOPIC, "Granted QoS = 0", 0)
    publish(client, TOPIC, "Granted QoS = 1", 1)
    publish(client, TOPIC, "Granted QoS = 2", 2)

    utils.waitfor(callback.messages, 3, 2)
    for message in callback.messages:
        msg = message["message"]
        print("[Received] Topic: %s, Payload: %s, QoS: %d" % (msg.topic, msg.payload.decode("utf-8"), msg.qos))

    client.disconnect()
    callback.wait_disconnected()
    client.loop_stop()
    callback.clear()

def no_local():
    # Subscription Options - No Local
    print("\nSubscription Options - No Local")
    print("--------------------------------")

    callback = utils.Callbacks()
    client = mqtt.Client(CLIENTID.encode("utf-8"), protocol = mqtt.MQTTv5)
    callback.register(client)
    client.connect(host = HOST, port = PORT, clean_start = True)
    client.loop_start()
    response = callback.wait_connected()

    # No Local = 0
    client.subscribe(TOPIC, options = SubscribeOptions(noLocal = False))
    callback.wait_subscribed()
    print("[Subscribe] No Local: 0")
    publish(client, TOPIC, "No Local = 0")
    utils.waitfor(callback.messages, 1, 2)
    msg = callback.messages[0]["message"]
    print("[Received] Topic: %s, Payload: %s" % (msg.topic, msg.payload.decode("utf-8")))

    callback.clear()

    # No Local = 1
    client.subscribe(TOPIC, options = SubscribeOptions(noLocal = True))
    callback.wait_subscribed()
    print("\n[Subscribe] No Local: 1")
    publish(client, TOPIC, "No Local = 1")
    utils.waitfor(callback.messages, 1, 2)
    if len(callback.messages) == 0:
        print("[Received] No message")

    client.disconnect()
    callback.wait_disconnected()
    client.loop_stop()
    callback.clear()

def retain_as_published():
    # Subscription Options - Retain As Published
    print("\nSubscription Options - Retain As Published")
    print("-------------------------------------------")

    utils.clean_retained_message(HOST, PORT, TOPIC)
    
    callback = utils.Callbacks()
    client = mqtt.Client(CLIENTID.encode("utf-8"), protocol = mqtt.MQTTv5)
    callback.register(client)
    client.connect(host = HOST, port = PORT, clean_start = True)
    client.loop_start()
    response = callback.wait_connected()

    # Retain As Published = 0
    client.subscribe(TOPIC, options = SubscribeOptions(qos = 2, retainAsPublished = False))
    callback.wait_subscribed()
    print("[Subscribe] Retain As Published: 0")
    publish(client, TOPIC, "Retain As Published = 0", retain = True)
    utils.waitfor(callback.messages, 1, 2)
    msg = callback.messages[0]["message"]
    print("[Received] Topic: %s, Payload: %s, Retain: %d" % (msg.topic, msg.payload.decode("utf-8"), msg.retain))

    client.unsubscribe(TOPIC)
    callback.wait_unsubscribed()
    callback.clear()

    utils.clean_retained_message(HOST, PORT, TOPIC)

    # Retain As Published = 1
    client.subscribe(TOPIC, options = SubscribeOptions(qos = 2, retainAsPublished = True))
    callback.wait_subscribed()
    print("\n[Subscribe] Retain As Published: 1")
    publish(client, TOPIC, "Retain As Published = 1", retain = True)
    utils.waitfor(callback.messages, 1, 2)
    msg = callback.messages[0]["message"]
    print("[Received] Topic: %s, Payload: %s, Retain: %d" % (msg.topic, msg.payload.decode("utf-8"), msg.retain))

    client.disconnect()
    callback.wait_disconnected()
    client.loop_stop()
    callback.clear()

def retain_handling():
    # Subscription Options - Retain Handling
    print("\nSubscription Options - Retain Handling")
    print("---------------------------------------")

    utils.clean_retained_message(HOST, PORT, TOPIC)

    callback = utils.Callbacks()
    client = mqtt.Client(CLIENTID.encode("utf-8"), protocol = mqtt.MQTTv5)
    callback.register(client)
    client.connect(host = HOST, port = PORT, clean_start = True)
    client.loop_start()
    response = callback.wait_connected()

    publish(client, TOPIC, "Retain Handling = 0", retain = True)
    time.sleep(1)

    # Retain Handling = 0
    client.subscribe(TOPIC, options = SubscribeOptions(retainHandling = 0))
    callback.wait_subscribed()
    print("\n[Subscribe] Retain Handling: 0")
    utils.waitfor(callback.messages, 1, 2)
    msg = callback.messages[0]["message"]
    print("[Received] Topic: %s, Payload: %s, Retain: %d" % (msg.topic, msg.payload.decode("utf-8"), msg.retain))

    callback.clear()

    client.subscribe(TOPIC, options = SubscribeOptions(retainHandling = 0))
    callback.wait_subscribed()
    print("[Subscribe] Retain Handling: 0")
    utils.waitfor(callback.messages, 1, 2)
    msg = callback.messages[0]["message"]
    print("[Received] Topic: %s, Payload: %s, Retain: %d" % (msg.topic, msg.payload.decode("utf-8"), msg.retain))

    client.unsubscribe(TOPIC)
    callback.wait_unsubscribed()
    callback.clear()

    # Retain Handling = 1
    client.subscribe(TOPIC, options = SubscribeOptions(retainHandling = 1))
    callback.wait_subscribed()
    print("\n[Subscribe] Retain Handling: 1")
    utils.waitfor(callback.messages, 1, 2)
    msg = callback.messages[0]["message"]
    print("[Received] Topic: %s, Payload: %s, Retain: %d" % (msg.topic, msg.payload.decode("utf-8"), msg.retain))

    callback.clear()

    client.subscribe(TOPIC, options = SubscribeOptions(retainHandling = 1))
    callback.wait_subscribed()
    print("[Subscribe] Retain Handling: 1")
    utils.waitfor(callback.messages, 1, 2)
    if len(callback.messages) == 0:
        print("[Received] No message")

    client.unsubscribe(TOPIC)
    callback.wait_unsubscribed()
    callback.clear()

    # Retain Handling = 2
    client.subscribe(TOPIC, options = SubscribeOptions(retainHandling = 2))
    callback.wait_subscribed()
    print("\n[Subscribe] Retain Handling: 2")
    utils.waitfor(callback.messages, 1, 2)
    if len(callback.messages) == 0:
        print("[Received] No message")

    client.disconnect()
    callback.wait_disconnected()
    client.loop_stop()
    callback.clear()

if __name__=="__main__":
    qos()
    no_local()
    retain_as_published()
    retain_handling()
    utils.clean_retained_message(HOST, PORT, TOPIC)
