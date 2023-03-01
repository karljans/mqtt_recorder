import sys
import time
import struct

from paho.mqtt import client as mqtt

class MqttRecorder:
    def __init__(self, mqtt_client: mqtt, topics: list, quiet: bool = False) -> None:
        """
        MQTT recorder class. Monitors MQTT traffic and saves received data to a file

        Args:
            mqtt_client (mqtt): Paho MQTT client object instance
            topics (list): List of topics to monitor
            quiet (bool): If True, info messages are not printed on screen, only debug messages
        """
        
        self.mqtt_client = mqtt_client
        self.topics = topics
        self.quiet = quiet
        
        self.mqtt_file = None

        self.first_frame = True
        self.start_time = 0
        self.duration = 0
        self.count = 0

        self.terminate = False
       
        
    def message_callback(self, client: mqtt, userdata: dict, 
                         message: mqtt.MQTTMessage) -> None:
        """
        Callback for MQTT messages

        Args:
            client (mqtt): Mqtt client instance
            userdata (dict): User data passed to MQTT
            message (mqtt.MQTTMessage): Received messsage
        """

        if self.first_frame:
            self.first_frame = False
            self.start_time = time.time()

        # Encode topic name
        topic_bs = bytearray(message.topic, 'iso-8859-15')

        # Encode message
        msg_bs = bytearray(message.payload)

        # Calculate legths of different message components
        topic_len = struct.pack('<I', len(topic_bs))
        msg_len = struct.pack('<I', len(msg_bs))

        # Build MQTT message entry
        mqtt_bytes = b"".join([topic_len, topic_bs, msg_len, msg_bs])

        timestamp = time.time() - self.start_time
        timestamp_packed = struct.pack('<d', timestamp)

        mqtt_entry = b"".join([timestamp_packed, mqtt_bytes])
        mqtt_len = struct.pack('<I', len(mqtt_entry))

        # MQTT message entry header
        file_data = b"".join([mqtt_len, mqtt_entry])

        # Save the MQTT entry
        with open(self.mqtt_file, 'ab') as fp:
            fp.write(file_data)

        # Append count in user data
        self.count += 1

        # Append duration in user data
        self.duration = timestamp

    def reset(self, mqtt_file: str) -> None:
        """
        Initializes the recorder class for new recording.
        Creates new MQTT file and resets the class

        Args:
            mqtt_file (str): Path of the file where to save the data
        """
        
        # Reset the class
        self.terminate = False
        self.first_frame = True
        
        self.start_time = 0
        self.duration = 0
        self.count = 0

        # Set the MQTT file where to record
        self.mqtt_file = mqtt_file
        
        # Write file header placeholder
        try:
            with open(self.mqtt_file, 'wb') as fp:
                # Write header text that identifies the file
                fp.write("MQTTv1.0".encode('ascii'))

                # Reserve 8 bytes for number of messages (will be filled later)
                fp.write(struct.pack('<Q', 0))

                # Reserve 8 bytes for length of the recoding (in seconds) (will be filled later)
                fp.write(struct.pack('<d', 0))

        except IOError as e:
            print(f"ERROR: Could not open MQTT file for writing: {str(e)}", file=sys.stderr)
            exit(1)

        self.mqtt_client.on_message = self.message_callback

        self.mqtt_client.loop_start()

    def stop(self) -> None:
        """
        Stops recording
        """

        self.terminate = True
        
        if not self.quiet:
            print("Writing file header")

        # Fill missing file header data
        try:
            with open(self.mqtt_file, 'r+b') as fp:

                # Write number of messages to file header
                fp.seek(8)
                fp.write(struct.pack('<Q', self.count))

                fp.seek(16)
                fp.write(struct.pack('<d', self.duration))
                
        except IOError as e:
            print(f"Could not open MQTT file for writing: {str(e)}", file=sys.stderr)
            exit(1)

        # Make sure we don't accidentally overwrite the file
        self.mqtt_file = None

        if not self.quiet:
            print(f"Logged {self.count} messages")
        