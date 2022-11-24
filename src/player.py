import sys
import time
import datetime
from pyclbr import Function

from bitstring import ConstBitStream
from paho.mqtt import client as mqtt


class MqttPlayer:
    def __init__(self, mqtt_file: str, valid_topics: list,
                 invalid_topics: list, mqtt_client: mqtt,
                 publish: bool = True, callback: Function = None,
                 cb_user_data: object = None,
                 quiet: bool = False) -> None:
        """
        MQTT file player. Reads saved MQTT stream and publishes the contents through an MQTT broker

        Args:
            mqtt_file (str): MQTT file tor read
            valid_topics (list): List of topics in the file to publish
            invalid_topics (list): List of topics in the file to ignore
            mqtt_client (mqtt): Instance of Paho MQTT client object
            publish (bool, optional): Weather to publish the data read from the file. 
                                      Defaults to True.
            callback (Function, optional): Optional callback function to call when a 
                                           message is read form the file. Defaults to None.
            cb_user_data (object, optional): User data to pass to the callback function, optional. 
                                             Defaults to None.
            quiet (bool): If True, the progress messages are not printed

        """

        self.mqtt_file = mqtt_file
        self.valid_topics = valid_topics
        self.invalid_topics = invalid_topics
        self.mqtt_client = mqtt_client

        self.publish = publish
        self.callback = callback
        self.cb_user_data = cb_user_data
        self.quiet = quiet

        self.terminate = False
        self.first_msg = True
        self.start_time = 0

    def _is_topic_valid(self, topic: str) -> bool:
        """
        Checks if a topic should be published

        Args:
            topic (str): Name of the topic

        Returns:
            bool: True if topic should be published, False otherwise
        """

        # TODO: This is kind of messy, works though
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

    def reset(self) -> None:
        """
        Resets the class
        """
        self.first_msg = True
        self.start_time = 0

    def run(self) -> int:
        """
        Parses an MQTT file and publishes the read messages

        Returns:
            int: 0 if no errors occurred, 1 otherwise
        """
        counter = 0

        print()

        # Save the starting time
        if self.first_msg:
            self.start_time = time.time()
            self.first_msg = False

        # Open file for reading
        try:
            bitstream = ConstBitStream(filename=self.mqtt_file)

        except (FileNotFoundError, IOError) as e:
            print(f"Could not open MQTT file for reading: {str(e)}", file=sys.stderr)
            exit(1)

        # File header
        try:
            file_hdr = bitstream.read('bytes:8').decode('ascii')
        except UnicodeDecodeError:
            file_hdr = ''

        if file_hdr != "MQTTv1.0":
            print("Error reading file: unknown file format!", file=sys.stderr)
            return 1

        # Total number of messages
        msg_count = bitstream.read('uintle:64')
        print("Total number of messages in file:", msg_count)

        duration = bitstream.read('floatle:64')
        duration_str = str(datetime.timedelta(seconds=duration))
        print(f"Total duration of the recording: {duration_str}")

        print("\nPlaying", self.mqtt_file, end='\n\n')
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

            if self._is_topic_valid(topic):
                # Wait to synchronize the messages
                curr_time = time.time() - self.start_time

                while (curr_time < timestamp) and not self.terminate:
                    curr_time = time.time() - self.start_time

                    if not self.quiet:
                        time_str = str(datetime.timedelta(seconds=curr_time))
                        print(f"{time_str} of {duration_str} ({round(curr_time * 100 / duration, 2):.2f} %)", end='\r')

                if self.terminate:
                    print("\nCaught interrupt signal, exiting")
                    return 0

                counter += 1
                # print(f"Message {counter} of {msg_count} ({round(counter * 100 / msg_count, 2)} %)", end='\r')

                # Publish the message
                if self.publish:
                    self.mqtt_client.publish(topic, msg)

                # Run the custom callback function, if specified
                if self.callback is not None:
                    self.callback(msg_count, counter, timestamp, topic, msg, self.cb_user_data)

        print()
        print("End of file")
        return 0

    def stop(self) -> None:
        """
        Stops the playback
        """
        self.terminate = True
