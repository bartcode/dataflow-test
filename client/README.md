# Message client
This simple client can be used to send messages to test the (Scala/Scio) code of this repository.

## Installation
```bash
$ conda env create -f environment.yml -p venv/
$ conda activate venv/
```
Update `application.conf` with the required parameters (self-explanatory). **And don't forget to run
the pip installation again after updating.**

## Running the code
```
$ pubsend --help

usage: pubsend [-h] [-p PROJECT_ID] [-t TOPIC] [-l LOOPS]
                    [-c MESSAGE_COUNT] [-s SLEEP_TIME]

Send messages to Pub/Sub

optional arguments:
  -h, --help            show this help message and exit
  -p PROJECT_ID, --project_id PROJECT_ID
                        GCP project ID.
  -t TOPIC, --topic TOPIC
                        Pub/Sub topic.
  -l LOOPS, --loops LOOPS
                        Amount of loops to go through.
  -c MESSAGE_COUNT, --message_count MESSAGE_COUNT
                        Number of messages to send.
  -s SLEEP_TIME, --sleep_time SLEEP_TIME
                        Seconds of sleep between message bursts.
```
