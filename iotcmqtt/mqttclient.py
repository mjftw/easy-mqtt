import paho.mqtt.client as mqtt
import time

class MqttClient:
    ''' Listen to a given topic on an MQTT network and run a callback on recive
    Args:
        topic (:obj:`str`): Topic branch to subscuribe to
            in order to recieve messages.
        on_message (function): This callaback is run when a message is recieved
            from the MQTT broker on the subscribed topic.
            >>> def on_message(client, userdata, message):
                ...
        on_connect (function, optional): This callback is run on connection to
            the MQTT Broker.
            >>> def on_connect(client, userdata, flags, rc):
                ...
        on_disconnect (function, optional): This callback is run on
            disconnection from the MQTT Broker.
            >>> def on_disconnect(client, userdata, rc):
                ...
        broker_host (:obj:`str`, optional): Host address of MQTT broker server.
            Default is 127.0.0.1 (localhost).
        broker_port (int, optional): Connection port of MQTT broker server.
            Default is 1883 (default unsecured MQTT port)
        client_id (:obj:`str`, optional): An ID string to be used by the client
            when connecting to the broker. By default the MAC address of the
            machine will be used.
        birth_message (dict, optional): A dict containing parameters for the client's
            birth message. This message is sent by the client upon connection.
            birth_message = {
                ‘topic’: “<topic>”,
                ‘payload’:”<payload”>,
                ‘qos’:<qos>,
                ‘retain’:<retain>
            }
            Topic is required, other parameters are optional and will default
            to None, 0, and False respectively.
            Defaults to sending client connection status:
            {
                'topic': f'clients/<client_id>/connected',
                'payload': '1',
                'qos': 1,
                'retain': True
            }
        will_message (dict, optional): A dict containing parameters for the client's
            last will and testiment. This message is sent to other clients by the
            broker if the client disconnects unexpectedly.
            will_message = {
                ‘topic’: “<topic>”,
                ‘payload’:”<payload”>,
                ‘qos’:<qos>,
                ‘retain’:<retain>
            }
            Topic is required, other parameters are optional and will default
            to None, 0, and False respectively.
            Defaults to sending client connection status:
            {
                'topic': f'clients/<client_id>/connected',
                'payload': '0',
                'qos': 1,
                'retain': True
            }
        threaded (bool, optional): Should the listener run in a separate
            thread? If false, connect(...) method is a blocking call.
            Default is True
    '''
    def __init__(self, topic, on_message, on_connect=None, on_disconnect=None,
                 broker_host=None, broker_port=None, client_id=None,
                 username=None, password=None,
                 birth_message=None, will_message=None, threaded=None):
        self.topic = topic

        assert callable(on_message)
        self.on_message = on_message

        if on_connect:
            assert callable(on_connect)
        self.on_connect = on_connect

        if on_disconnect:
            assert callable(on_disconnect)
        self.on_disconnect = on_disconnect

        self.broker_host = broker_host or '127.0.0.1'
        self.broker_port = broker_port or 1883
        self.client_id = client_id

        if (password and not username):
            raise AttributeError(
                'Must provide username if using password')

        self.username = username
        self.password = password

        # Set birth message
        if birth_message:
            if 'topic' not in birth_message:
                raise AttributeError('birth_message must have "topic" key')
            if 'payload' not in birth_message:
                birth_message['payload'] = None
            if 'qos' not in birth_message:
                birth_message['qos'] = 0
            if 'retain' not in birth_message:
                birth_message['retain'] = False

        if not birth_message:
            birth_message = {
                'topic': f'clients/{self.client_id}/connected',
                'payload': '1',
                'qos': 1,
                'retain': True
            }

        self.birth_message = birth_message

        # Set will message
        if will_message:
            if 'topic' not in will_message:
                raise AttributeError('will_message must have "topic" key')
            if 'payload' not in will_message:
                will_message['payload'] = None
            if 'qos' not in will_message:
                will_message['qos'] = 0
            if 'retain' not in will_message:
                will_message['retain'] = False

        if not will_message:
            will_message = {
                'topic': f'clients/{self.client_id}/connected',
                'payload': '0',
                'qos': 1,
                'retain': True
            }

        self.will_message = will_message

        self.threaded = threaded or True

        self._connected = False

        # MQTT client
        self._client = None

    @property
    def connected(self):
        ''' Is client connected to MQTT broker? '''
        return self._connected

    def connect(self):
        ''' Connect to MQTT broker and start publishing sensor values '''
        if not self._connected:
            self._start_client()

    def disconnect(self):
        ''' Disconnect from MQTT broker and stop reading sensor '''
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
        self._client = None

    def _start_client(self):
        self._client = mqtt.Client(
            client_id=self.client_id
        )

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self.on_message

        if self.will_message:
            self._client.will_set(
                topic=self.will_message['topic'],
                payload=self.will_message['payload'],
                qos=self.will_message['qos'],
                retain=self.will_message['retain']
            )

        if self.username:
            self._client.username_pw_set(
                username=self.username,
                password=self.password
            )

        self._client.connect(
            host=self.broker_host,
            port=self.broker_port
        )

        if self.threaded:
            self._client.loop_start()
        else:
            self._client.loop_forever()

        # Spin until connected
        timeout = 5
        seconds_passed = 0
        while not self.connected:
            if seconds_passed > timeout:
                raise ConnectionError('Timeout waiting to connect to MQTT broker')
            time.sleep(0.1)
            seconds_passed += 0.1

        self._client.subscribe(self.topic, 2)

    def _on_connect(self, *args, **kwargs):
        self._connected = True

        # Publish birth message
        if self.birth_message:
            self._client.publish(
                topic=self.birth_message['topic'],
                payload=self.birth_message['payload'],
                qos=self.birth_message['qos'],
                retain=self.birth_message['retain']
            )

        if self.on_connect:
            self.on_connect(*args, **kwargs)

    def _on_disconnect(self, *args, **kwargs):
        self._connected = False
        if self.on_disconnect:
            self.on_disconnect(*args, **kwargs)


def get_mac_address():
    ''' Helper function to get MAC address of machine and format it nicely '''
    import uuid
    import re

    return ':'.join(re.findall('..', '%012x' % uuid.getnode()))