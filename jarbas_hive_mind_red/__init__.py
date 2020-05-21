from jarbas_hive_mind.master import HiveMind, HiveMindProtocol
from jarbas_hive_mind import HiveMindListener
from jarbas_utils.log import LOG
from jarbas_utils.messagebus import Message
import json
import base64

platform = "NodeRedMindV0.1"


class NodeRedMindProtocol(HiveMindProtocol):
    @staticmethod
    def decode_auth(request):
        auth = request.headers.get("authorization")
        if auth is None:
            return super().decode_auth(request)
        decoded = str(base64.b64decode(auth.split()[1]), 'utf-8')
        name, key = decoded.split(":")

        return name, key


class NodeRedMind(HiveMind):
    protocol = NodeRedMindProtocol

    def __init__(self, debug=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.debug = debug

    # parsed protocol messages
    def nodered_send(self, message):
        if isinstance(message, Message):
            payload = Message.serialize(message)
        elif isinstance(message, dict):
            payload = repr(json.dumps(message))
        else:
            payload = message

        for peer in set(self.clients):
            client = self.clients[peer]["instance"]
            client.sendMessage(payload.encode())

    # HiveMind protocol messages -  from node red
    def on_message(self, client, payload, isBinary):
        """
       Process message from client, decide what to do internally here
       """
        client_protocol, ip, sock_num = client.peer.split(":")

        if isBinary:
            # TODO receive files
            pass
        else:
            data = json.loads(payload)
            msg_type = data.get("msg_type") or data.get("type", "")
            if msg_type.startswith("node_red."):
                data["context"]["source"] = client.peer
                data["context"]["platform"] = platform
                #data["context"]["destination"] = None  # broadcast
                if msg_type == 'node_red.query':
                    msg_type = "recognizer_loop:utterance"
                    data["context"]["destination"] = "skills"
                    self.mycroft_send(msg_type, data["data"], data["context"])
                elif msg_type in ['node_red.answer', 'node_red.speak']:
                    msg_type = "speak"
                    self.mycroft_send(msg_type, data["data"], data["context"])
                    self.mycroft_send("node_red.success", data["data"],
                                      data["context"])
                elif msg_type == 'node_red.converse.activate':
                    self.mycroft_send(msg_type, data["data"], data["context"])
                elif msg_type == 'node_red.converse.deactivate':
                    self.mycroft_send(msg_type, data["data"], data["context"])
                elif msg_type == 'node_red.intent_failure':
                    self.mycroft_send(msg_type, data["data"], data["context"])
                elif msg_type == 'node_red.pong':
                    self.mycroft_send(msg_type, data["data"], data["context"])
                elif msg_type == 'node_red.listen':
                    self.mycroft_send("mycroft.mic.listen", data["data"],
                                      data["context"])
            else:
                super().on_message(client, payload, isBinary)

    def handle_bus_message(self, payload, client):
        # Generate mycroft Message
        super().handle_bus_message(payload, client)
        # echo to nodered (all connections/flows)
        # TODO skip source peer
        self.nodered_send(message=Message("hivemind.bus", payload))

    def handle_broadcast_message(self, data, client):
        payload = data["payload"]

        LOG.info("Received broadcast message at: " + self.node_id)
        LOG.debug("ROUTE: " + str(data["route"]))
        LOG.debug("PAYLOAD: " + str(payload))
        # echo to nodered (all connections/flows)
        # TODO skip source peer
        self.nodered_send(message=Message("hivemind.broadcast", payload))

    def handle_propagate_message(self, data, client):

        payload = data["payload"]

        LOG.info("Received propagate message at: " + self.node_id)
        LOG.debug("ROUTE: " + str(data["route"]))
        LOG.debug("PAYLOAD: " + str(payload))

        # echo to nodered (all connections/flows)
        # TODO skip source peer
        self.nodered_send(message=Message("hivemind.propagate", payload))

    def handle_escalate_message(self, data, client):
        payload = data["payload"]

        LOG.info("Received escalate message at: " + self.node_id)
        LOG.debug("ROUTE: " + str(data["route"]))
        LOG.debug("PAYLOAD: " + str(payload))

        # echo to nodered (all connections/flows)
        # TODO skip source peer
        self.nodered_send(message=Message("hivemind.escalate", payload))

    # from mycroft bus
    def handle_outgoing_mycroft(self, message=None):
        if isinstance(message, dict):
            message = json.dumps(message)
        if isinstance(message, str):
            message = Message.deserialize(message)
        if message.msg_type == "complete_intent_failure":
            message.msg_type = "hive.complete_intent_failure"

        message.context = message.context or {}
        peer = message.context.get("destination")

        # if msg_type namespace is node_red
        # if message is for a node red connection, forward
        if message.msg_type.startswith("node_red."):
            self.nodered_send(message)
        elif peer and peer in self.clients:
            self.nodered_send(message)
        return


class NodeRedListener(HiveMindListener):
    def secure_listen(self, key=None, cert=None, factory=None, protocol=None):
        factory = factory or NodeRedMind(self.bus)
        protocol = protocol or NodeRedMindProtocol
        return super().secure_listen(key=key, cert=cert,
                                     factory=factory, protocol=protocol)

    def unsafe_listen(self, factory=None, protocol=None):
        factory = factory or NodeRedMind(self.bus)
        protocol = protocol or NodeRedMindProtocol
        return super().unsafe_listen(factory=factory, protocol=protocol)

    def listen(self, factory=None, protocol=None):
        factory = factory or NodeRedMind(self.bus)
        protocol = protocol or NodeRedMindProtocol
        return super().listen(factory=factory, protocol=protocol)


def get_listener(port=6789, max_connections=-1, bus=None):
    return NodeRedListener(port, max_connections, bus)
