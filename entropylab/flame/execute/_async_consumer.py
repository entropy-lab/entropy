# https://github.com/pika/pika/blob/main/examples/asynchronous_consumer_example.py

import pika
import json
import functools

from pika.channel import Channel
from pika.exchange_type import ExchangeType

# typing
from zmq import Context  # noqa: F401
from redis import Redis  # noqa: F401
from typing import Optional

# entropylab
from entropylab.flame.execute._config import (
    _Config,
    logger,
    node_debugging_user,
    node_debugging_password,
    port,
)
from entropylab.flame.execute._debug_publisher import DebugNodeOutput


class NodeOutputsConsumer(object):
    EXCHANGE = "output_debugging"
    EXCHANGE_TYPE = ExchangeType.topic
    CONSUME_QUEUE = "consume_output_debugging_queue"
    PUBLISH_QUEUE = "publish_output_debugging_queue"

    def __init__(
        self,
        runtime_state,
        zmq_context,
    ):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param Redis runtime_state: redis runtime state
        :param Context zmq_context: zmq context
        """
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel: Optional[Channel] = None
        self._closing = False
        self._consumer_tag = None
        self._url = (
            f"amqp://{node_debugging_user}:{node_debugging_password}"
            f"@localhost:{port}/%2F"
        )
        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1
        self.CONSUME_ROUTING_KEY = f"consume_debug_output.{_Config.runtime_id}"
        self.PUBLISH_ROUTING_KEY = (
            f"publish_debug_output.{_Config.runtime_id}.{_Config.job_eui}"
        )

        self.runtime_state = runtime_state
        self.zmq_context = zmq_context
        self.debug_output: Optional[DebugNodeOutput] = None

    def _connect(self):
        """Connects to RabbitMQ.

        :rtype: pika.SelectConnection
        :return: connection handle
        """
        logger.info("Connecting to %s", self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_open_error,
            on_close_callback=self._on_connection_closed,
        )

    def _close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            logger.info("Connection is closing or already closed")
        else:
            logger.info("Closing connection")
            self._connection.close()

    def _on_connection_open(self, _connection):
        """This method is called once the connection to RabbitMQ has
        been established.

        :param pika.SelectConnection _connection: The connection
        """
        logger.info("Connection opened")
        self._open_channel()

    def _on_connection_open_error(self, _connection, err):
        """This method is called pika if the connection to RabbitMQ can't be
        established.

        :param pika.SelectConnection _connection: The connection
        :param Exception err: The error
        """
        logger.error("Connection open failed: %s", err)
        self._reconnect()

    def _on_connection_closed(self, _connection, reason):
        """This method is invoked when the connection to RabbitMQ is closed
        unexpectedly. Since it is unexpected, we will reconnect to RabbitMQ if
        it disconnects.

        :param pika.connection.Connection _connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning("Connection closed, reconnect necessary: %s", reason)
            self._reconnect()

    def _reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def _open_channel(self):
        """Open a new channel by issuing the Channel.Open RPC command."""
        logger.info("Creating a new channel")
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        """This method is invoked when the channel has been opened. The channel
        object is passed in, so we can make use of it. Declare exchange to use.

        :param pika.channel.Channel channel: The channel object
        """
        logger.info("Channel opened")
        self._channel = channel
        self._add_on_channel_close_callback()
        self._setup_exchange(self.EXCHANGE)

    def _add_on_channel_close_callback(self):
        logger.info("Adding channel close callback")
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel, reason):
        """Invoked when RabbitMQ unexpectedly closes the channel.

        :param pika.channel.Channel channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        logger.warning("Channel %i was closed: %s", channel, reason)
        self._close_connection()

    def _setup_exchange(self, exchange_name):
        """Setup the exchange.

        :param str|unicode exchange_name: The name of the exchange to declare
        """
        logger.info("Declaring exchange: %s", exchange_name)
        cb = functools.partial(self._on_exchange_declare_ok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE.value,
            durable=True,
            callback=cb,
        )

    def _on_exchange_declare_ok(self, _frame, userdata):
        """Invoked when RabbitMQ has finished the Exchange.Declare RPC command.

        :param pika.Frame.Method _frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)
        """
        logger.info("Exchange declared: %s", userdata)
        self._setup_queue(self.PUBLISH_QUEUE, self.PUBLISH_ROUTING_KEY, False)
        self._setup_queue(self.CONSUME_QUEUE, self.CONSUME_ROUTING_KEY, True)

    def _setup_queue(self, queue_name, routing_key, use_bind_ok_cb=False):
        """Setup the queue by invoking the Queue.Declare RPC command.

        :param str|unicode queue_name: The name of the queue to declare.
        :param str|unicode routing_key: routing key
        :param bool use_bind_ok_cb: need create and pass on_bind_ok callback
        """
        logger.info("Declaring queue %s", queue_name)
        cb = functools.partial(
            self._on_queue_declare_ok,
            queue_name=queue_name,
            routing_key=routing_key,
            use_bind_ok_cb=use_bind_ok_cb,
        )
        self._channel.queue_declare(queue=queue_name, callback=cb)

    def _on_queue_declare_ok(
        self, _unused_frame, queue_name, routing_key, use_bind_ok_cb
    ):
        """Method invoked when the Queue.Declare RPC call made in setup_queue
        has completed. In this method we will bind the queue and exchange
        together with the routing key by issuing the Queue.Bind RPC command.

        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode queue_name: queue name
        :param str|unicode routing_key: routing key
        :param bool use_bind_ok_cb: need create and pass on_bind_ok callback
        """
        logger.info("Binding %s to %s with %s", self.EXCHANGE, queue_name, routing_key)
        cb = None
        if use_bind_ok_cb:
            cb = functools.partial(
                self._on_bind_ok, queue_name=queue_name, routing_key=routing_key
            )
        self._channel.queue_bind(
            queue_name, self.EXCHANGE, routing_key=routing_key, callback=cb
        )

    def _on_bind_ok(self, _unused_frame, queue_name, routing_key):
        """Invoked when the Queue.Bind method has completed.

        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
        :param str|unicode queue_name: queue name
        :param str|unicode routing_key: routing key

        """
        logger.info("Queue bound: %s. Routing key: %s", queue_name, routing_key)
        self._set_qos()

    def _set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one.
        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self._on_basic_qos_ok
        )

    def _on_basic_qos_ok(self, _unused_frame):
        """Invoked when the Basic.QoS method has completed. Will start consuming
        messages.

        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame
        """
        logger.info("QOS set to: %d", self._prefetch_count)
        if self._channel is None:
            logger.error("Can't create DebugNodeOutput")
            return
        self.debug_output = DebugNodeOutput(
            self.runtime_state,
            self.zmq_context,
            self._channel,
            self.PUBLISH_ROUTING_KEY,
            self.EXCHANGE,
        )
        self._start_consuming()

    def _start_consuming(self):
        """Sets up the consumer:
        1. add on cancel callback
        2. add on message callback
        """
        logger.info("Issuing consumer related RPC commands")
        self._add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.CONSUME_QUEUE, self.on_message, auto_ack=True
        )
        self.was_consuming = True
        self._consuming = True

    def _add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason.
        """
        logger.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

    def _on_consumer_cancelled(self, method_frame):
        """Invoked when RabbitMQ sends a Basic.Cancel for a consumer receiving
        messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame
        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        """Invoked when a message is delivered from RabbitMQ.

        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.Spec.Basic.Deliver basic_deliver: basic deliver method
        (carries the exchange, routing key, delivery tag and a redelivered flag)
        :param pika.Spec.BasicProperties properties: message properties
        :param bytes body: The message body
        """
        try:
            msg = json.loads(body)
        except json.JSONDecodeError:
            raise ValueError("Message wasn't decoded")
        event = msg.get("event")
        if event == "sub":
            output_name = msg.get("output_name", "")
            rate = msg.get("rate", 0.5)
            logger.info(f"Get message. Subscribe. Name: {output_name}; rate: {rate}")
            self.debug_output.start(output_name, rate)
            self._channel.basic_publish(
                self.EXCHANGE,
                self.PUBLISH_ROUTING_KEY,
                f"event: {event}; output: {output_name}".encode(),
            )
        elif event == "unsub":
            self.debug_output.stop()
        else:
            logger.error("Unknown message type")

    def _stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            cb = functools.partial(self._on_cancel_ok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def _on_cancel_ok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)
        """
        self._consuming = False
        logger.info(
            "RabbitMQ acknowledged the cancellation of the consumer: %s", userdata
        )
        self._close_channel()

    def _close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.
        """
        logger.info("Closing the channel")
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.
        """
        self._connection = self._connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, _on_cancel_ok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.
        """
        logger.info("Async consumer stop")
        if not self._closing:
            self._closing = True
            logger.info("Stopping")
            if self._consuming:
                self._stop_consuming()
            self._connection.ioloop.stop()
            self.debug_output.stop()
            logger.info("Stopped")
