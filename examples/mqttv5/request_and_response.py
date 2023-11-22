import utils

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from utils import print_in_progress, print_success, print_fail, highlight

HOST = "broker.emqx.io"
PORT = 1883

REQUESTER = 0
RESPONDER = 1

def test_request_and_response(host, port):

    callbacks = []
    clients = []
    topics = ["state/light-in-bedroom/power", "command/light-in-bedroom/power", "new/state/light-in-bedroom/power"]

    # Client 0 - command/... -> MQTT Server - command/... -> Client 1
    # Client 0 <- state/... --- MQTT Server <- state/... --- Client 1
    # So Client 0 subscribe to state/..., Client 1 subscribe to command/...
    for i in range(2):
        callback = utils.Callbacks()
        clientid = utils.random_clientid()
        client = mqtt.Client(clientid.encode("utf-8"), protocol = mqtt.MQTTv5)
        callback.register(client)

        client.connect(host = host, port = port, clean_start = True)
        client.loop_start()
        response = callback.wait_connected()
        if response["reasonCode"] != 0:
            print_fail("Client %s connect to %s failed due to %s." %
                    (highlight(i), highlight(host), highlight(response["reasonCode"])))
            exit()
        print_success("Client %s connected to %s" % (highlight(i), highlight(host)))

        client.subscribe(topics[i], qos = 2)
        response = callback.wait_subscribed()
        if response["reasonCodes"][0].getId(response["reasonCodes"][0].getName()) > 2:
            print_fail("Client %s subscribe to %s failed due to %s" %
                        (highlight(i), highlight(topics[i]), highlight(response["reasonCodes"][0])))
            exit()
        print_in_progress("Client %s subscribed to %s" % (highlight(i), highlight(topics[i])))

        callbacks.append(callback)
        clients.append(client)

    print("")

    request_and_response(
        clients,
        callbacks,
        request_topic = topics[1],
        response_topic = topics[0],
        correlation_data = "123",
        payload = "ON"
    )

    print("")

    clients[REQUESTER].unsubscribe(topics[0])
    callbacks[REQUESTER].wait_unsubscribed()
    clients[REQUESTER].subscribe(topics[2])
    callbacks[REQUESTER].wait_subscribed()
    print_success("Client %s unsubscribed %s, subscribed %s" % 
                      (highlight(REQUESTER), highlight(topics[0]), highlight(topics[2])))
    
    request_and_response(
        clients,
        callbacks,
        request_topic = topics[1],
        # Use a new Response Topic
        response_topic = topics[2],
        correlation_data = "124",
        payload = "ON"
    )

def request_and_response(clients, callbacks, request_topic, response_topic, correlation_data, payload):
    # Set Response Topic and Correlation Data
    properties = Properties(PacketTypes.PUBLISH)
    properties.ResponseTopic = response_topic
    properties.CorrelationData = bytes(correlation_data, encoding = "utf-8")
    clients[REQUESTER].publish(request_topic, payload, 0, properties = properties)
    print_success("Client %s published to %s: %s, Response Topic = %s" % 
                  (highlight(REQUESTER), highlight(request_topic), highlight(payload), highlight(response_topic)))
    
    messages = callbacks[RESPONDER].wait_messages(count = 1, timeout = 2)
    msg = messages[0]["message"]
    print_success("Client %s received a request from %s: %s" % 
                      (highlight(RESPONDER), highlight(msg.topic), highlight(msg.payload.decode("utf-8"))))
    if hasattr(msg.properties, 'ResponseTopic'):
        # Return Correlation Data in the response
        properties = Properties(PacketTypes.PUBLISH)
        if hasattr(msg.properties, 'CorrelationData'):
            properties.CorrelationData = msg.properties.CorrelationData
        clients[RESPONDER].publish(msg.properties.ResponseTopic, payload, 0, properties = properties)
        print_success("Client %s published to %s: %s" % 
                  (highlight(RESPONDER), highlight(msg.properties.ResponseTopic), highlight(payload)))
    
    messages = callbacks[REQUESTER].wait_messages(count = 1, timeout = 2)
    msg = messages[0]["message"]
    # Check whether the response contains associated data and is consistent with the request
    if hasattr(msg.properties, 'CorrelationData'):
        if msg.properties.CorrelationData.decode("utf-8") == correlation_data:
            print_success("Client %s received a response from %s: %s, with correct correlation data" % 
                        (highlight(REQUESTER), highlight(msg.topic), highlight(msg.payload.decode("utf-8"))))
        else:
            print_fail("Client %s received a response from %s with bad correlation data" %
                       (highlight(REQUESTER), highlight(msg.topic)))
    else:
        print_fail("Client %s received a response from %s without correlation data" %
                    (highlight(REQUESTER), highlight(msg.topic)))

if __name__ == "__main__":
    test_request_and_response(HOST, PORT)


