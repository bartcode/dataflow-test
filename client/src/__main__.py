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

    message_count = 10
    loops = 1

    for j in range(loops):
        logging.info('Loop %d/%d, sending %d messages at timestamp %s.', j + 1, loops, message_count,
                     datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        number_sum = 0
        for k in range(message_count):
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

        if j + 1 != loops:
            logging.info('Done! Waiting 5 seconds.')

            time.sleep(5)


if __name__ == '__main__':
    main()
