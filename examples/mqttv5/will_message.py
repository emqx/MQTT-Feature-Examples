import time
import utils

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCodes

from utils import print_in_progress, print_success, print_fail, highlight

HOST = "broker.emqx.io"
PORT = 1883

WILL_RECIPIENT = 3

def test_will_message(host, port):
    callbacks = []
    clients = []
    will_topics = []

    for i in range(0, 4):
        callback = utils.Callbacks()
        clientid = utils.random_clientid()
        client = mqtt.Client(clientid.encode("utf-8"), protocol = mqtt.MQTTv5)
        callback.register(client)

        if i != WILL_RECIPIENT:
            will_topic = clientid + "/will"
            will_properties = Properties(PacketTypes.WILLMESSAGE)
            will_properties.WillDelayInterval = 0
            client.will_set(will_topic, payload = "This is a will message from Client " + str(i), properties = will_properties)

        callbacks.append(callback)
        clients.append(client)
        will_topics.append(will_topic)

    for i in range(0, 4):
        clients[i].connect(host = host, port = port, clean_start = True)
        clients[i].loop_start()
        response = callbacks[i].wait_connected()
        if response["reasonCode"] != 0:
            print_fail("Client %s connect to %s failed due to %s." %
                    (highlight(i), highlight(host), highlight(response["reasonCode"])))
            exit()
        print_success("Client %s connected to %s" % (highlight(i), highlight(host)))

    for i in range(0, 3):
        clients[WILL_RECIPIENT].subscribe(will_topics[i], qos = 2)
        response = callbacks[WILL_RECIPIENT].wait_subscribed()
        if response["reasonCodes"][0].getId(response["reasonCodes"][0].getName()) > 2:
            print_fail("Client %s subscribe to %s failed due to %s" %
                    (highlight(WILL_RECIPIENT), highlight(will_topics[i]), highlight(response["reasonCodes"][0])))
            exit()
        print_in_progress("Client %s subscribed to %s" % (highlight(WILL_RECIPIENT), highlight(will_topics[i])))

    print("")

    # 130, Protocol Error
    clients[0].disconnect(reasoncode = ReasonCodes(PacketTypes.DISCONNECT, identifier = 130))
    callbacks[0].wait_disconnected()
    clients[0].loop_stop()
    print_success("Client 0 disconnectd with Protocol Error")

    messages = callbacks[WILL_RECIPIENT].wait_messages(count = 1, timeout = 2)
    msg = messages[0]["message"]
    if msg.topic == will_topics[0]:
        print_success("Client %s received the will message, the content is: %s" %
                      (highlight(WILL_RECIPIENT), highlight(msg.payload.decode())))

    clients[1].disconnect()
    callbacks[1].wait_disconnected()
    clients[1].loop_stop()
    print_success("Client 1 disconnectd with Success")

    messages = callbacks[WILL_RECIPIENT].wait_messages(count = 1, timeout = 2)
    if len(messages) == 0:
        print_success("Client %s did not receive any will messages" % (highlight(WILL_RECIPIENT)))

    clients[2].publish("", "hello", 2)
    callbacks[2].wait_disconnected()
    clients[2].loop_stop()
    print_success("Client 2 was disconnectd due to Protocol Error")

    messages = callbacks[WILL_RECIPIENT].wait_messages(count = 1, timeout = 2)
    msg = messages[0]["message"]
    if msg.topic == will_topics[2]:
        print_success("Client %s received the will message, the content is: %s" %
                      (highlight(WILL_RECIPIENT), highlight(msg.payload.decode())))


    clients[WILL_RECIPIENT].disconnect()
    callbacks[WILL_RECIPIENT].wait_disconnected()
    clients[WILL_RECIPIENT].loop_stop()


if __name__ == "__main__":
    test_will_message(HOST, PORT)


