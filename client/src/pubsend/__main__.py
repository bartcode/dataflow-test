"""
Send messages to PubSub.
"""
import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime

from google.cloud import pubsub_v1
from pyhocon import ConfigFactory

from .message.message_pb2 import NumberBuffer

logging.basicConfig(level=logging.INFO)

CONFIG = ConfigFactory().parse_file(os.path.join(sys.prefix, 'pubsend', 'application.conf'))

PARSER = argparse.ArgumentParser(description='Send messages to Pub/Sub')

PARSER.add_argument(
    '-p', '--project_id',
    type=str,
    default=CONFIG.get('project.project_id'),
    help='GCP project ID.'
)
PARSER.add_argument(
    '-t', '--topic',
    type=str,
    default=CONFIG.get('project.topic'),
    help='Pub/Sub topic.'
)
PARSER.add_argument(
    '-l', '--loops',
    type=int,
    default=CONFIG.get('project.loops'),
    help='Amount of loops to go through.'
)
PARSER.add_argument(
    '-c', '--message_count',
    type=int,
    default=CONFIG.get('project.message_count'),
    help='Number of messages to send.'
)
PARSER.add_argument(
    '-s', '--sleep_time',
    type=int,
    default=CONFIG.get('project.sleep_time'),
    help='Seconds of sleep between message bursts.'
)

PARSER.add_help = True


def main() -> None:
    """
    Send messages
    :return: None
    """
    args, _ = PARSER.parse_known_args()

    publisher = pubsub_v1.PublisherClient()
    topic_name = f'projects/{args.project_id}/topics/{args.topic}'

    for loop_index in range(args.loops):
        logging.info('Loop %d/%d, sending %d messages at timestamp %s.', loop_index + 1, args.loops, args.message_count,
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        number_sum = 0

        for _ in range(args.message_count):
            number = random.randint(1, 4)
            number_sum += number
            message = NumberBuffer(
                id=random.randint(1e6, 1e9),
                timestamp=round(datetime.timestamp(datetime.now()) * 1000),
                name='None',
                number=number,
                type='None'
            )

            # logging.info('Publishing message %d: %s.', k, message)
            publisher.publish(topic_name, data=message.SerializeToString())

        logging.info('Sum for this round equals %d.', number_sum)

        if loop_index + 1 != args.loops:
            logging.info('Done! Waiting %d seconds.', args.sleep_time)

            time.sleep(args.sleep_time)


if __name__ == '__main__':
    main()
