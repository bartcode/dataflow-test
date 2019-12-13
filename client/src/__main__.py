"""
Send messages to PubSub.
"""
import logging
import random
import time
from datetime import datetime

from google.cloud import pubsub_v1
from src.message.message_pb2 import NumberBuffer

logging.basicConfig(level=logging.INFO)


def main() -> None:
    """
    Do it.
    :return: None
    """
    project_id = 'playground-bart'
    topic = 'sensor'

    publisher = pubsub_v1.PublisherClient()
    topic_name = f'projects/{project_id}/topics/{topic}'

    for j in range(5):
        logging.info('Loop %d, sending 10k messages at timestamp %s.', j, datetime.timestamp(datetime.now()))
        for k in range(10000):
            message = NumberBuffer(
                id=random.randint(1e6, 1e9),
                timestamp=round(datetime.timestamp(datetime.now()) * 1000),
                name='None',
                number=random.randint(1, 4),
                type='None'
            )

            # logging.info('Publishing message %d: %s.', k, message)
            publisher.publish(topic_name, data=message.SerializeToString())

        logging.info('Done! Waiting 5 seconds.')

        time.sleep(5)


if __name__ == '__main__':
    main()
