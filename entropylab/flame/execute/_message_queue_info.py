import os
import pika
from pika.exchange_type import ExchangeType

from entropylab.flame.execute._config import logger


user = os.environ.get("FLAME_MESSAGING_USER_NAME", "flame_user")
password = os.environ.get("FLAME_MESSAGING_USER_PASS", "flame_password")
port = os.environ.get("FLAME_MESSAGING_PORT", "5672")


def _setup_message_queue(host_url: str = "localhost"):
    logger.debug("MessageQueueInfo. Setup message queue")
    status_connection = pika.BlockingConnection(
        pika.URLParameters(f"amqp://{user}:{password}@{host_url}:{port}")
    )
    updates_channel = status_connection.channel()
    updates_channel.exchange_declare(
        "amq.topic", exchange_type=ExchangeType.topic, durable=True
    )
    return status_connection, updates_channel


class MessageQueueInfo:
    def __init__(self):
        try:
            self.status_connection, self.updates_channel = _setup_message_queue(
                "localhost"
            )
            logger.info("MessageQueueInfo. Host: localhost")
        except Exception:
            try:
                self.status_connection, self.updates_channel = _setup_message_queue(
                    "messagequeue"
                )
                logger.info("MessageQueueInfo. Host: messagequeue")
            except Exception:
                self.status_connection, self.updates_channel = None, None
                logger.info("MessageQueueInfo. No message queue configured.")
