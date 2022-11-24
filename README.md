# MQTT Recorder

A simple tool that allows to record MQTT messages into a file and later play the messages back from that file, with proper timings. It uses a custom binary format for saving the data. Think of it as a rosbag recorder / player, but for MQTT traffic.

The tool provides the user a command line interface, similar to `mosquitto_pub` and `mosquitto_sub` tools and allows the user to select which topics to record or play. It supports recording and playing of both text-based and binary MQTT messages.

Currently only unencrypted MQTT communication is supported, however, the tool can talk over unencrypted channels to an MQTT broker that requires a username and password for login. Support of encryption might be added at a later date.

## Requirements

This tool needs **Python 3.7** or newer to run, as it uses some language features that are not available in older versions of the language.

It requires the following third party packages:

* `paho-mqtt`
* `bitstring`

The packages can be installed using pip:

`pip3 install paho-mqtt bitstring`

## Usage

Basic usage:

`python3 mqtt_rec.py (--rec | --play | --info) <FILE> [options]`

A list of all command line arguments is shown below.

### Mode Selection

Exactly one mode should be specified at any time

| Argument      | Description                                                           |
| ------------- | --------------------------------------------------------------------- |
| `--rec FILE`  | Record MQTT data to the file FILE                                     |
| `--play FILE` | Play MQTT data from the file FILE                                     |
| `--info FILE` | Displays basic information about the data stored in an MQTT file FILE |
| `--help`      | Displays usage for the tool                                           |

### Connecting to broker

The following flags can be used for connecting to the MQTT broker. All flags are optional, if no info is specified, it is assumed that the broker is running on `localhost` (`127.0.0.1`), port `1883`.

| Argument                    | Description                             | Default     |
| --------------------------- | --------------------------------------- | ----------- |
| `-h HOST`, `--host HOST`    | Host running MQTT broker                | `127.0.0.1` |
| `-p PORT`, `--port PORT`    | Port the MQTT broker is running at      | `1883`      |
| `-u USER`, `--user USER`    | Username for MQTT connection (optional) |             |
| `-P PASSW`, `--passw PASSW` | Password for MQTT connection (optional) |

### Topic selection

The flags below allow to filter MQTT topics to record or to publish while playing the file. If no topic selection is specified, the tool will record or play all topics (`#`).

| Argument                        | Description                                                                          |
| ------------------------------- | ------------------------------------------------------------------------------------ |
| `-t TOPIC`, `--topic TOPIC`     | Specifies a topic to record or play. This flag can be used multiple times            |
| `-T TOPIC`, `--no-topics TOPIC` | Specifies a topic to **NOT** to record or play. This flag can be used multiple times |

### Playback Control

Flags to control the playback of MQTT data. Can only be used together with the `--play` flag.

| Argument        | Description                                                                              |
| --------------- | ---------------------------------------------------------------------------------------- |
| `-l`, `--loop`  | Continue playing the file from the beginning once the end of reached, instead of exiting |
| `-q`, `--quiet` | Quiet mode, does not print out progress info. Useful for running as a background process |

## Custom MQTT File Format Description

The tool uses a custom binary file format for storing the data. The file uses little endian data encoding.

Every file has a file header, with length of 24 bytes. The header is followed by data section. The data section consists of data frames. Each frame represents a single MQTT message with additional metadata, such as the topic where it was published, timestamp, etc.

``` text
+--------+-------+-------+     +-------+
| Header | Frm 0 | Frm 1 | ... | Frm N |
+--------+-------+-------+     +-------+
```

### File Header

The header has the following format:

``` text
24         16            8                 0
+----------+--------------+-----------------+
| MQTTv1.0 | Count (uint) | Duration (float)|
+----------+--------------+-----------------+
```

| Field      | Length                 | Description                              |
| ---------- | ---------------------- | ---------------------------------------- |
| Identifier | 8 bytes                | String `MQTTv1.0`                        |
| Count      | 8 bytes (unsigned int) | Number of data frames stored in the file |
| Duration   | 8 bytes (64-bit float) | Length of the recording, in seconds      |

### Data Section

The header is followed by the data section. Data section consists of data frames. The total amount of these frames is specified by the `count` field in the file header.

Each data frame consists of the frame header and the MQTT message itself:

``` text
 --- 4 bytes --- ------- 8 bytes ------ - MsgLength -
+---------------+----------------------+-------------+
| MsgLen (uint) | MsgTimestamp (float) |     Msg     |
+---------------+----------------------+-------------+
```

Description of the message header fields is shown in the table below

| Field        | Length                 | Description                                                                |
| ------------ | ---------------------- | -------------------------------------------------------------------------- |
| MsgLen       | 4 bytes (unsigned int) | Length of the MQTT message segment that follows the frame header, in bytes |
| MsgTimestamp | 8 bytes (64-bit float) | Time in seconds since the first recorded message                           |

Each message contains the topic name where it was published and the published data. It also includes the lengths of the fields, as shown below:

``` text
 ---- 4 bytes ---- -- TopicLen -- ---- 4 bytes ---- -- DataLen --
+-----------------+--------------+-----------------+-------------+
| TopicLen (uint) | Topic (str)  | DataLen (uint)  |    Data     |
+-----------------+--------------+-----------------+-------------+
```

| Field    | Length                 | Description                                             |
| -------- | ---------------------- | ------------------------------------------------------- |
| TopicLen | 4 bytes (unsigned int) | Length of the following topic name section, in bytes    |
| Topic    | TopicLen bytes         | Topic name, using `iso-8859-15` encoding                |
| DataLen  | 4 bytes (unsigned int) | Length of the following data section, in bytes          |
| Data     | DataLen Bytes          | MQTT message data, stored as raw binary string of bytes |
