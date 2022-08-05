#!/bin/env python3

import argparse
import os
import signal
import struct
import sys
import time

from bitstring import ConstBitStream
from paho.mqtt import client as mqtt

START_TIME = time.time()

should_quit = False

def sigint_handler(signal, frame):
    global should_quit
    should_quit = True
    print('Cought KeyboardInterrupt, exiting')


def mqtt_play(mqtt_file: str, mqtt_client: mqtt) -> None:
    global should_quit

    counter = 0

    bitstream = ConstBitStream(filename=mqtt_file)
    
    try:
        file_hdr = bitstream.read('bytes:6').decode('ascii')
    except UnicodeDecodeError:
        file_hdr = ''
    
    if file_hdr != "MQTTv1":
        print("Error reading file: unknown file format!", file=sys.stderr)
        sys.exit(1)
    
    print(file_hdr)
    
    msg_count = bitstream.read('uintle:64')
    print("Total count in file:", msg_count)

    while bitstream.pos < bitstream.length and not should_quit:

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

        # Wait to synchronize the messages
        while (time.time() - START_TIME < timestamp) and not should_quit:
            pass

        counter += 1
        print(f"{round(counter * 100 / msg_count, 2)} %")

        # Publish the message
        mqtt_client.publish(topic, msg)

    should_quit = True


def message_callback(client: mqtt, userdata: dict, message: mqtt.MQTTMessage) -> None:
    """
    Callback for MQTT messages

    Args:
        client (mqtt): Mqtt client instance
        userdata (dict): User data passed to MQTT
        message (mqtt.MQTTMessage): Received messsage
    """
    
    # Encode topic name
    topic_bs = bytearray(message.topic, 'iso-8859-15')

    # Encode message
    msg_bs = bytearray(message.payload)

    # Calculate legths of different message components
    topic_len = struct.pack('<I', len(topic_bs))
    msg_len = struct.pack('<I', len(msg_bs))

    # Build MQTT message entry
    mqtt_bytes = b"".join([topic_len, topic_bs, msg_len, msg_bs])
    timestamp = struct.pack('<d', time.time() - START_TIME)

    mqtt_entry = b"".join([timestamp, mqtt_bytes])
    mqtt_len = struct.pack('<I', len(mqtt_entry))

    # MQTT message entry header
    file_data = b"".join([mqtt_len, mqtt_entry])

    # Save the MQTT entry
    with open(userdata['file'], 'ab') as fp:
        fp.write(file_data)

    # Append count in user data
    userdata['count'] += 1


def mqtt_on_connect_callback(client: mqtt, userdata: dict, flags: dict, rc) -> None:
    """
    Callback that is run when connection to the MQTT broker is made

    Args:
        client (_type_): Mqtt client instance (not used)
        userdata (_type_): User data passed to MQTT (not used)
        flags (_type_): Response flags sent by the broker
        rc (_type_): Return code given by the broker connection
    """
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", mqtt.connack_string(rc))


def arg_parser(arguments_passed: bool) -> argparse:
    """
    Parses command line arguments

    Args:
        arguments_passed (bool): True if any arguments were passed, 
                                 False if no arguments were passed

    Returns:
        argparse: Argparse object containing info regarding the passed arguments
                  None if no arguments were passed
    """

    parser = argparse.ArgumentParser(
        description='MQTT message recorder and player', add_help=False)

    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument(
        '--rec', help='Record MQTT traffic into this file')
    action_group.add_argument(
        '--play', help='Play MQTT traffic from this file')

    broker_group = parser.add_argument_group("MQTT Broker information")
    broker_group.add_argument('-h', '--host', default='127.0.0.1',
                              help='Host running MQTT broker. Defaualts to localhost')

    broker_group.add_argument('-p', '--port', type=int, default=1883,
                              help='Port the MQTT broker is running at. Defaults to 1883')

    broker_group.add_argument(
        '-u', '--user', help='Provides a username for MQTT connection')
    broker_group.add_argument(
        '-P', '--passw', help='Provides a password for MQTT connection')

    topic_group = parser.add_argument_group("MQTT topics")
    topic_group.add_argument('-t', '--topics', default='#',
                             help='Comma separated list of MQTT topics to subscribe.'
                             'Defaults to all topics (#)')

    topic_group.add_argument('-T', '--no-topics',
                             help='Comma separated list of MQTT topics to filter out')

    parser.add_argument('-nh', '--help', action='help', default=argparse.SUPPRESS,
                        help='Show this help message and exit.')

    args = parser.parse_args()

    if not arguments_passed:
        parser.print_help()
        return None

    return args


def record(mqtt_file: str, mqtt_client: mqtt, userdata) -> None:
    global should_quit

    # Write file header
    with open(mqtt_file, 'wb') as fp:
        # Write header text that identifies the file        
        fp.write("MQTTv1".encode('ascii'))
        
        # Reserve 8 bytes for number of messages (will be filled later)
        fp.write(struct.pack('<Q', 0))

    mqtt_client.on_message = message_callback
    mqtt_client.subscribe(args.topics)  # TODO: support multiple topics
    mqtt_client.loop_start()

    # Block until KeyboardInterrupt
    while not should_quit:
        pass

    print("exiting")
    if args.rec:
        with open(mqtt_file, 'r+b') as fp:
            
            # 
            fp.seek(6)
            fp.write(struct.pack('<Q', userdata['count']))

        print(f"Logged {userdata['count']} messages")


def main(args: argparse) -> int:

    global should_quit
    mqtt_file = None

    if args.play:
        mqtt_file = os.path.abspath(os.path.expanduser(args.play))

    elif args.rec:
        mqtt_file = os.path.abspath(os.path.expanduser(args.rec))

    if mqtt_file == None:
        print("File is None. This should never happen", file=sys.stderr)
        return 1

    # MQTT client
    userdata = {
        'file': mqtt_file,
        'count': 0
    }
    mqtt_client = mqtt.Client(f'MQTT-recorder', userdata=userdata)

    # Set up MQTT connection
    if args.user and args.passw:
        mqtt_client.username_pw_set(args.user, args.passw)

    mqtt_client.connect(args.host, args.port)
    mqtt_client.on_connect = mqtt_on_connect_callback

    # Register KeyboardInterrupt handler
    signal.signal(signal.SIGINT, sigint_handler)

    if args.rec:
        record(mqtt_file, mqtt_client, userdata)

    elif args.play:
        mqtt_play(mqtt_file, mqtt_client)

    return 0


if __name__ == '__main__':
    ret = 0

    arguments_passed = len(sys.argv) > 1
    args = arg_parser(arguments_passed)

    if args:
        ret = main(args)

    sys.exit(ret)
