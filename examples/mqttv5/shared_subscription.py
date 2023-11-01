import time
import utils

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from utils import print_in_progress, print_success, print_fail, highlight

HOST = "broker.emqx.io"
PORT = 1883

def test_shared_subscription_in_mqtt5(host, port):
    callbacks = []
    clients = []

    pub_topic = utils.random_string()
    sub_topic = '$share/group1/' + pub_topic
    subscriptions = [sub_topic, sub_topic, pub_topic]

    for i in range(0, 3):
        callback = utils.Callbacks()
        clientid = utils.random_clientid()
        client = mqtt.Client(clientid.encode("utf-8"), protocol = mqtt.MQTTv5)
        callback.register(client)

        client.connect(host = host, port = port, clean_start = True)
        print_in_progress("Client %s connecting to %s" % (highlight(i), highlight(host)))
        client.loop_start()
        response = callback.wait_connected()
        if response["reasonCode"] != 0:
            print_fail("Client %s connect to %s failed due to %s." %
                    (highlight(i), highlight(host), highlight(response["reasonCode"])))
            exit()
        print_success("Client %s connected" % highlight(i))

        client.subscribe(subscriptions[i], qos = 2)
        print_in_progress("Client %s subscribing to %s" % (highlight(i), highlight(subscriptions[i])))
        response = callback.wait_subscribed()
        if response["reasonCodes"][0].getId(response["reasonCodes"][0].getName()) > 2:
            print_fail("Client %s subscribe to %s failed due to %s" %
                    (highlight(i), highlight(subscriptions[i]), highlight(response["reasonCodes"][0])))
            exit()

        print_success("Client %s subscribed to %s" % (highlight(i), highlight(subscriptions[i])))

        print("")
        callbacks.append(callback)
        clients.append(client)

    count = 5
    for i in range(count):
        clients[0].publish(pub_topic, "message " + str(i), 0)

    waited = 0
    while len(callbacks[0].messages) + len(callbacks[1].messages) < count and waited < 10:
        time.sleep(.5)
        waited += 1

    messages_recv_by_client0_and_1 = len(callbacks[0].messages) + len(callbacks[1].messages)
    messages_recv_by_client2 = len(callbacks[2].wait_messages(count, 5))

    print_in_progress("Messages received by client 0 & 1: %s" % highlight(messages_recv_by_client0_and_1))
    print_in_progress("Messages received by client 2: %s" % highlight(messages_recv_by_client2))

    for i in range(0, 3):
        clients[i].disconnect()
        callbacks[i].wait_disconnected()
        clients[i].loop_stop()
        print_success("Client %s disconnectd" % highlight(i))

if __name__ == "__main__":
    test_shared_subscription_in_mqtt5(HOST, PORT)