import sys
import time
from pyclbr import Function

from bitstring import ConstBitStream
from paho.mqtt import client as mqtt


class MqttPlayer:
    def __init__(self, mqtt_file: str, valid_topics: list,
                 invalid_topics: list, mqtt_client: mqtt,
                 publish: bool = True, callback: Function = None,
                 cb_user_data: object = None) -> None:

        self.mqtt_file = mqtt_file
        self.valid_topics = valid_topics
        self.invalid_topics = invalid_topics
        self.mqtt_client = mqtt_client

        self.publish = publish
        self.callback = callback
        self.cb_user_data = cb_user_data

        self.terminate = False
        self.first_msg = True
        self.start_time = 0

    def _topic_valid(self, topic):
        if len(self.valid_topics) > 0:
            if (topic in self.valid_topics) and (topic not in self.invalid_topics):
                return True
            else:
                return False

        else:
            if topic not in self.invalid_topics:
                return True
            else:
                return False

    def run(self) -> int:
        counter = 0

        # Save the starting time
        if self.first_msg:
            self.start_time = time.time()
            self.first_msg = False

        # Open file for reading
        bitstream = ConstBitStream(filename=self.mqtt_file)

        # File header
        try:
            file_hdr = bitstream.read('bytes:6').decode('ascii')
        except UnicodeDecodeError:
            file_hdr = ''

        if file_hdr != "MQTTv1":
            print("Error reading file: unknown file format!", file=sys.stderr)
            return 1

        # Total number of messages
        msg_count = bitstream.read('uintle:64')
        print("Total number of messages in file:", msg_count)

        while bitstream.pos < bitstream.length and not self.terminate:

            # Read the mqtt entry
            mqtt_len = bitstream.read('uintle:32')
            timestamp = bitstream.read('floatle:64')

            mqtt_bs = ConstBitStream(bitstream.read(f'bytes:{mqtt_len - 8}'))

            # Read topic name
            topic_len = mqtt_bs.read('uintle:32')
            topic = mqtt_bs.read(f'bytes:{topic_len}').decode('iso-8859-15')

            # Read message data
            msg_len = mqtt_bs.read('uintle:32')
            msg = mqtt_bs.read(f'bytes:{msg_len}')

            if self._topic_valid(topic):
                # Wait to synchronize the messages
                while (time.time() - self.start_time < timestamp) and not self.terminate:
                    pass

                counter += 1
                print(f"{round(counter * 100 / msg_count, 2)} %")

                # Publish the message
                if self.publish:
                    self.mqtt_client.publish(topic, msg)

                # Run the custom callback function, if specified
                if self.callback is not None:
                    self.callback(msg_count, counter, timestamp, topic, msg, self.cb_user_data)

        return 0

    def stop(self):
        self.terminate = True
