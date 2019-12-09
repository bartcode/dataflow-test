"""
Send messages to PubSub.
"""
import logging
import random
from datetime import datetime

from google.cloud import pubsub_v1
from src.message_pb2 import NumberBuffer

logging.basicConfig(level=logging.INFO)


def main() -> None:
    """
    Do it.
    :return: None
    """
    project_id = 'playground-bart'
    topic = 'manual'

    publisher = pubsub_v1.PublisherClient()
    topic_name = f'projects/{project_id}/topics/{topic}'

    for k in range(100):
        message = NumberBuffer(
            id=random.randint(1e6, 1e9),
            timestamp=round(datetime.timestamp(datetime.now()) * 1000),
            name='None',
            number=random.randint(1, 10000),
            type='None'
        )

        logging.info('Publishing message %d: %s.', k, message)
        publisher.publish(topic_name, data=message.SerializeToString())


if __name__ == '__main__':
    main()
