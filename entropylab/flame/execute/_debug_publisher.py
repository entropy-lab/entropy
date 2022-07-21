import time
import json
import threading
from typing import Optional

import zmq
import redis
import msgpack

from pika.adapters.blocking_connection import BlockingChannel

from entropylab.flame.execute._config import logger
from entropylab.flame.execute._utils import send_amqp_message
from entropylab.flame.utils.zmq import (
    create_socket_and_connect_or_bind,
    connect_or_bind,
)


class DebugNodeOutput:
    def __init__(
        self,
        runtime_state: redis.Redis,
        zmq_context: zmq.Context,
        updates_channel: BlockingChannel,
        routing_key: str,
        exchange: str,
    ):
        self.runtime_state = runtime_state
        self.zmq_context = zmq_context
        self.publisher_channel = updates_channel
        self.routing_key = routing_key
        self.exchange = exchange

        self.is_running = False
        self.socket: Optional[zmq.Socket] = None
        self.address: Optional[str] = None
        self.output_name: Optional[str] = None
        self.thread: Optional[threading.Thread] = None

    def _subscribe(self, output_name: str):
        address = self.runtime_state.get(output_name)
        if address is None:
            logger.error(
                "Subscribe to: %s failed; output_name '%s' has no zmq address",
                self.address,
                self.output_name,
            )
            raise ValueError(f"'{output_name}' has no zmq address.")
        self.address = address
        self.output_name = output_name
        logger.debug(
            "Subscribe to: %s; output_name: %s", self.address, self.output_name
        )
        if self.socket is None:
            self.socket = create_socket_and_connect_or_bind(
                self.zmq_context,
                zmq.SUB,
                address,
                connect=True,
                socket_options={zmq.CONFLATE: 1, zmq.LINGER: 0},
                socket_options_string={zmq.SUBSCRIBE: ""},
            )
        else:
            self.socket = connect_or_bind(self.socket, address, connect=True)

    def _unsubscribe(self):
        logger.debug(
            "Unsubscribe from: %s; output_name: %s", self.address, self.output_name
        )
        if self.is_running:
            self.socket.disconnect(self.address)
        self.is_running = False
        self.output_name = None
        self.address = None

    def _publish(self, socket: zmq.Socket, rate: float = 0.5):
        logger.debug(
            "Start publish to routing key: %s. Rate: %f", self.routing_key, rate
        )
        while self.is_running:
            start = time.monotonic()
            try:
                msg = socket.recv(zmq.NOBLOCK)
                unpacked_msg = msgpack.unpackb(msg)
                logger.debug("Publish message: %s", msg)
                send_amqp_message(
                    self.publisher_channel,
                    self.routing_key,
                    json.dumps(
                        {"output_name": self.output_name, "message": unpacked_msg}
                    ),
                    self.exchange,
                )
            except zmq.ZMQError:
                pass
            execution_time = time.monotonic() - start
            delta = rate - execution_time
            if delta > 0:
                time.sleep(delta)

    def start(self, output_name: str, rate: float = 0.5):
        logger.debug(
            "Start messages listening. Output name: %s, rate: %f", output_name, rate
        )
        if self.is_running:
            self.stop()
        self._subscribe(output_name)
        self.thread = threading.Thread(
            target=self._publish,
            args=(self.socket, rate),
        )
        self.thread.start()
        self.is_running = True

    def stop(self):
        logger.debug("Stop messages listening.")
        self._unsubscribe()
        if self.is_running:
            self.thread.join()
