import pika
from pika.exchange_type import ExchangeType

# typing
from typing import Union
from pika.channel import Channel
from pika.adapters.blocking_connection import BlockingChannel

from entropylab.flame.execute._config import logger, flame_user, flame_password, port


def _setup_message_queue(user: str, password: str, host_url: str = "localhost"):
    logger.debug("MessageQueueInfo. Setup message queue")
    status_connection = pika.BlockingConnection(
        pika.URLParameters(f"amqp://{user}:{password}@{host_url}:{port}")
    )
    channel = status_connection.channel()

    return status_connection, channel


def _exchange_declare(
    channel: Union[Channel, BlockingChannel],
    exchange: str = "amq.topic",
    exchange_type=ExchangeType.topic,
    durable=True,
):
    channel.exchange_declare(exchange, exchange_type=exchange_type, durable=durable)


class MessageQueueInfo:
    def __init__(self):
        try:
            self.connection, self.channel = _setup_message_queue(
                flame_user, flame_password, "localhost"
            )
            logger.info("MessageQueueInfo. Host: localhost")
        except Exception:
            try:
                self.connection, self.channel = _setup_message_queue(
                    flame_user, flame_password, "messagequeue"
                )
                logger.info("MessageQueueInfo. Host: messagequeue")
            except Exception:
                self.connection, self.channel = None, None
                logger.info("MessageQueueInfo. No message queue configured.")
        _exchange_declare(self.channel)
