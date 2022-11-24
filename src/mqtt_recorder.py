import struct
import time

from paho.mqtt import client as mqtt


def message_callback(client: mqtt, userdata: dict, message: mqtt.MQTTMessage) -> None:
    """
    Callback for MQTT messages

    Args:
        client (mqtt): Mqtt client instance
        userdata (dict): User data passed to MQTT
        message (mqtt.MQTTMessage): Received messsage
    """

    if userdata['first_frame']:
        userdata['first_frame'] = False
        userdata['start_time'] = time.time()

    # Encode topic name
    topic_bs = bytearray(message.topic, 'iso-8859-15')

    # Encode message
    msg_bs = bytearray(message.payload)

    # Calculate legths of different message components
    topic_len = struct.pack('<I', len(topic_bs))
    msg_len = struct.pack('<I', len(msg_bs))

    # Build MQTT message entry
    mqtt_bytes = b"".join([topic_len, topic_bs, msg_len, msg_bs])
    timestamp = struct.pack('<d', time.time() - userdata['start_time'])

    mqtt_entry = b"".join([timestamp, mqtt_bytes])
    mqtt_len = struct.pack('<I', len(mqtt_entry))

    # MQTT message entry header
    file_data = b"".join([mqtt_len, mqtt_entry])

    # Save the MQTT entry
    with open(userdata['file'], 'ab') as fp:
        fp.write(file_data)

    # Append count in user data
    userdata['count'] += 1


class MqttRecorder:
    def __init__(self, mqtt_file: str, mqtt_client: mqtt, topics: list) -> None:
        self.mqtt_file = mqtt_file
        self.mqtt_client = mqtt_client
        self.topics = topics

        self.userdata = {
            'file': mqtt_file,
            'first_frame': True,
            'start_time': 0,
            'count': 0
        }

        mqtt_client.user_data_set(self.userdata)

        self.terminate = False

    def stop(self):
        self.terminate = True

    def run(self) -> int:

        # Write file header
        with open(self.mqtt_file, 'wb') as fp:
            # Write header text that identifies the file
            fp.write("MQTTv1".encode('ascii'))

            # Reserve 8 bytes for number of messages (will be filled later)
            fp.write(struct.pack('<Q', 0))

        self.mqtt_client.on_message = message_callback

        for topic in self.topics:
            self.mqtt_client.subscribe(topic)

        self.mqtt_client.loop_start()

        # Block until KeyboardInterrupt
        while not self.terminate:
            pass

        with open(self.mqtt_file, 'r+b') as fp:

            # Write number of messages to file header
            fp.seek(6)
            fp.write(struct.pack('<Q', self.userdata['count']))

        print(f"Logged {self.userdata['count']} messages")

        return 0
