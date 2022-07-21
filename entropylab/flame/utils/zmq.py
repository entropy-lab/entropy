import zmq
from typing import Optional, Dict


def connect_or_bind(
    socket: zmq.Socket,
    address: str,
    bind: bool = False,
    connect: bool = False,
    subscribe_topic: Optional[str] = None,
):
    """Create zmq socket with specified parameters.

    One of params `bind` or `connect` must be set to True.

    :param socket: socket
    :param address: connection address
    :param bind: bind the socket to an address
    :param connect: connect to a remote 0MQ socket
    :param subscribe_topic: subscribe to a topic (only for SUB sockets)"""
    if bind + connect != 1:
        raise ValueError("One and only one of `bind` or `connect` must be True")
    if bind:
        socket.bind(address)
    else:
        socket.connect(address)
    if subscribe_topic is not None:
        if socket.type != zmq.SUB:
            raise ValueError("subscribe parameter allowed only for SUB sockets")
        socket.subscribe(subscribe_topic)
    return socket


def create_socket_and_connect_or_bind(
    zmq_context: zmq.Context,
    s_type: int,
    address: str,
    bind: bool = False,
    connect: bool = False,
    socket_options: Optional[Dict[int, int]] = None,
    socket_options_string: Optional[Dict[int, str]] = None,
    subscribe_topic: Optional[str] = None,
) -> zmq.Socket:
    """
    Create zmq socket with specified parameters.

    One of params `bind` or `connect` must be set to True.

    :param zmq_context: 0MQ context
    :param s_type: socket type (`zmq.SocketType`)
    :param address: connection address
    :param bind: bind the socket to an address
    :param connect: connect to a remote 0MQ socket
    :param socket_options: mapping of socket options (`zmq.SocketOption`)
        and it's int values
    :param socket_options_string: mapping of socket options (`zmq.SocketOption`)
        and it's int values
    :param subscribe_topic: subscribe to a topic (only for SUB sockets)
    """
    socket = zmq_context.socket(s_type)
    if socket_options is not None:
        for opt, value in socket_options.items():
            socket.setsockopt(opt, value)
    if socket_options_string is not None:
        for opt, value in socket_options_string.items():
            socket.setsockopt_string(opt, value)
    return connect_or_bind(socket, address, bind, connect, subscribe_topic)
