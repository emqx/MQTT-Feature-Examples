import utils

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from utils import print_in_progress, print_success, print_fail, highlight

HOST = "broker.emqx.io"
PORT = 1883

class ClientWithoutHeartbeat(mqtt.Client):
    def _send_pingreq(self):
        # Do nothing
        return 0

def _disconnect(client, callback):
    client.disconnect()
    callback.wait_disconnected()
    client.loop_stop()
    callback.clear()

def test_keep_alive(host, port, keepalive = 2):
    print("\n Test Keep Alive")
    print("------------------")

    callback = utils.Callbacks()
    clientid = utils.random_clientid()
    client = ClientWithoutHeartbeat(clientid.encode("utf-8"), protocol = mqtt.MQTTv5)
    callback.register(client)

    client.connect(host = host, port = port, clean_start = True, keepalive = keepalive)
    print_in_progress("Connecting to %s with Keep Alive = %s." % (highlight(host), highlight(keepalive)))
    client.loop_start()
    response = callback.wait_connected()
    if response["reasonCode"] != 0:
        print_fail("Connect to %s failed due to %s." %
                   (highlight(host), highlight(response["reasonCode"])))
        exit()
    print_success("Connected.")

    print_in_progress("Wait, and no heartbeat packets will be sent.")
    timeout = keepalive * 1.5 + 1
    callback.wait_disconnected(timeout)
    print_success("The server disconnected within %s seconds." % highlight(timeout))

    client.loop_stop()
    callback.clear()

def test_server_keep_alive(host, port):
    print("\n Test Server Keep Alive")
    print("-------------------------")

    callback = utils.Callbacks()
    clientid = utils.random_clientid()
    client = ClientWithoutHeartbeat(clientid.encode("utf-8"), protocol = mqtt.MQTTv5)
    callback.register(client)
    
    # 2 Hours
    keepalive = 2 * 60 * 60
    client.connect(host = host, port = port, clean_start = True, keepalive = keepalive)
    print_in_progress("Connecting to %s with Keep Alive = %s" % (highlight(host), highlight(keepalive)))
    client.loop_start()
    response = callback.wait_connected()
    if response["reasonCode"] != 0:
        print_fail("Connect to %s failed due to %s" %
                   (highlight(host), highlight(response["reasonCode"])))
        exit()

    if not hasattr(response["properties"], 'ServerKeepAlive'):
        print_fail("Connected. But MQTT Broker does not return Server Keep Alive.")
        _disconnect(client, callback)
        exit()

    server_keepalive = response["properties"].ServerKeepAlive
    if server_keepalive > 10:
        print_fail("Please reduce the Server Keep Alive to less than 10 seconds.")
        _disconnect(client, callback)
        exit()
    
    print_success("Connected. Server Keep Alive = %s." % highlight(server_keepalive))

    print_in_progress("Wait, and no heartbeat packets will be sent.")
    timeout = server_keepalive * 1.5 + 1
    callback.wait_disconnected(timeout)
    print_success("The server disconnected within %s seconds." % highlight(timeout))

    client.loop_stop()
    callback.clear()
    

if __name__=="__main__":

    is_server_keep_alive_enabled = False

    if is_server_keep_alive_enabled == False:
        # If Server Keep Alive is disabled, run this function
        test_keep_alive(HOST, PORT, 2)
    else:
        # If Server Keep Alive is enabled, run this function
        test_server_keep_alive(HOST, PORT)
