#!/bin/env python3

import argparse
import os
import random
import signal
import sys
import string

from paho.mqtt import client as mqtt

from src.player import MqttPlayer
from src.recorder import MqttRecorder


def arg_parser(arguments_passed: bool) -> argparse:
    """
    Parses command line arguments

    Args:
        arguments_passed (bool): True if any arguments were passed, 
                                False if no arguments were passed

    Returns:
        argparse.Namespace: Argparse object containing info regarding the passed arguments
                            None if no arguments were passed
    """

    parser = argparse.ArgumentParser(
        description='MQTT message recorder and player', add_help=False)

    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--rec', help='Record MQTT traffic into this file')
    action_group.add_argument('--play', help='Play MQTT traffic from this file')
    action_group.add_argument('--info', help='Show info about data stored in this file')

    broker_group = parser.add_argument_group("MQTT Broker information")
    broker_group.add_argument('-h', '--host', default='127.0.0.1',
                              help='Host running MQTT broker. Defaults to localhost')

    broker_group.add_argument('-p', '--port', type=int, default=1883,
                              help='Port the MQTT broker is running at. Defaults to 1883')

    broker_group.add_argument('-u', '--user', help='Provides a username for MQTT connection')
    broker_group.add_argument('-P', '--passw', help='Provides a password for MQTT connection')

    topic_group = parser.add_argument_group("MQTT topics")
    topic_group.add_argument('-t', '--topics', action='append', nargs='+',
                             help='MQTT topic to subscribe to. Can be used multiple times')

    topic_group.add_argument('-T', '--no-topics', action='append', nargs='+',
                             help='MQTT topics to filter out. Can be used multiple times')

    control_group = parser.add_argument_group("Program control")
    control_group.add_argument('-l', '--loop', action='store_true',
                               help='Continue playing the file from the beginning once the end of reached, '
                                    'instead of exiting the program')

    control_group.add_argument('-q', '--quiet', action='store_true',
                               help='Quiet mode, does not print out progress info. '
                                    'Useful for running as a background process')
    
    parser.add_argument('--help', action='help', default=argparse.SUPPRESS,
                        help='Show this help message and exit')

    args = parser.parse_args()

    if not arguments_passed:
        parser.print_help()
        return None

    return args

class App:
    def __init__(self, args: argparse.Namespace) -> None:
        """
        MQTT reader / recorder application class

        Args:
            args (argparse.Namespace): Argparse object containing info regarding the passed arguments
                                       None if no arguments were passe
        """

        self.args = args
        self.mqtt_class = None
        self.terminate = False

        # Register KeyboardInterrupt handler
        signal.signal(signal.SIGINT, lambda signal, frame: self._signal_handler())

        # Register Termination signal handler
        signal.signal(signal.SIGTERM, lambda signal, frame: self._signal_handler())

    def _signal_handler(self) -> None:
        """
        Stops the application in case of signal from the OS
        """         

        if self.mqtt_class is None:
            print('ERROR: Signal handler: MQTT class not initialized', file=sys.stderr)
            exit(1)

        self.terminate = True
        self.mqtt_class.stop()

    def _flatten_list(self, src: list) -> list:
        """
        Reads a list of lists and converts it into a single list
        containing all elements of all sub-lists

        Args:
            src (list): List of lists to flatten

        Returns:
            list: Flattened list
        """
        return [item for sublist in src for item in sublist]

    def _mqtt_on_connect_callback(self, client: mqtt, userdata: dict, flags: dict, rc: int) -> None:
        """
        Callback that is run when connection to the MQTT broker is made

        Args:
            client mqtt: Mqtt client instance (not used)
            userdata dict: User data passed to MQTT (not used)
            flags flags: Response flags sent by the broker (not used)
            rc int: Return code given by the broker connection
        """
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(f"Failed to connect, return code {mqtt.connack_string(rc)}")

    def main(self) -> int:
        """
        Main function, sets up inputs and runs the correct function (play / record)

        Returns:
            int: 0 if no errors occurred, 1 otherwise
        """

        mqtt_file = None

        # Argument processing
        if self.args.play:
            mqtt_file = args.play

        elif self.args.rec:
            mqtt_file = args.rec

        elif self.args.info:
            mqtt_file = args.info

        else:
            print("No mode selected. This should never happen", file=sys.stderr)
            return 1

        if mqtt_file == None:
            print("File is None. This should never happen", file=sys.stderr)
            return 1

        # Convert file path absolute path
        mqtt_file = os.path.abspath(os.path.expanduser(mqtt_file))

        if self.args.loop and not self.args.play:
            print("Warning: --loop flag cannot be used in record mode. Ignoring the --loop flag", 
                  file=sys.stderr)


        random_str = ''.join(random.choice(string.ascii_letters) for _ in range(32))
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, f'MQTT-bag-{random_str}')

        # Set up MQTT connection
        if self.args.user and self.args.passw:
            mqtt_client.username_pw_set(args.user, args.passw)

        mqtt_client.connect(args.host, args.port)
        mqtt_client.on_connect = self._mqtt_on_connect_callback

        # Process the list of specified topics
        no_topics_flat = self._flatten_list(
            self.args.no_topics) if self.args.no_topics else []

        topics_flat = []

        # Default if no topics are specified
        if not args.topics:
            if args.rec:
                print("Note: No topics specified, subscribing to all ('#')")
                topics_flat.append('#')

        # Only keep the topics that are not in args.no_topics
        else:
            for topic in self.args.topics:
                if topic[0] not in no_topics_flat:
                    topics_flat.append(topic[0])

        # We are recording
        if self.args.rec:
            # If the user used the no-topics flag to remove all
            # subcritions, including to '#'.
            if len(topics_flat) == 0:
                print("Error! No topics specified")
                return 1

            self.mqtt_class = MqttRecorder(
                mqtt_file, mqtt_client, topics_flat)

        # We are playing
        elif args.play:

            self.mqtt_class = MqttPlayer(
                mqtt_file, topics_flat, no_topics_flat, 
                mqtt_client, quiet=args.quiet)

        elif args.info:

            self.mqtt_class = MqttPlayer(
                mqtt_file, topics_flat, no_topics_flat, 
                mqtt_client, quiet=args.quiet, info_mode=True)

        # Finally run MQTT record / play

        ret = 0

        if args.play and args.loop:
            while (not self.terminate and ret == 0):
                self.mqtt_class.reset()
                ret = self.mqtt_class.run()
        else:
            ret = self.mqtt_class.run()

        return ret


if __name__ == '__main__':

    # Parse arguments
    arguments_passed = len(sys.argv) > 1
    args = arg_parser(arguments_passed)

    # Run the application
    ret = 0
    if args:
        app = App(args)
        ret = app.main()

    sys.exit(ret)
