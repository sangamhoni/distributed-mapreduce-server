"""Networking code shared by the Manager and the Worker.

Every message is a JSON object.  Every TCP message uses a new connection:
connect, send the whole message, close.
"""
import json
import logging
import socket


LOGGER = logging.getLogger(__name__)


def tcp_send(address, message):
    """Send one JSON message to address=(host, port) over a new connection.

    Raises ConnectionRefusedError if nothing is listening there.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect(address)
        sock.sendall(json.dumps(message).encode("utf-8"))


def udp_send(address, message):
    """Send one JSON message to address=(host, port) as a UDP datagram."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(address)
        sock.sendall(json.dumps(message).encode("utf-8"))


def receive_all(clientsocket):
    """Read bytes from a TCP connection until the sender closes it."""
    clientsocket.settimeout(1)
    with clientsocket:
        message_chunks = []
        while True:
            try:
                data = clientsocket.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            message_chunks.append(data)
    return b"".join(message_chunks)


def decode_message(message_bytes):
    """Return the message as a dict, or None if it isn't valid JSON."""
    try:
        message = json.loads(message_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(message, dict):
        return None
    return message


def tcp_server(address, shutdown_event, handle_func, on_listen=None):
    """Handle TCP messages on address until shutdown_event is set.

    Call handle_func(message) for each valid message.  If given, call
    on_listen() once the socket is listening.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
        sock.listen()
        sock.settimeout(1)
        LOGGER.info("Listening on TCP port %s", address[1])

        if on_listen is not None:
            on_listen()

        while not shutdown_event.is_set():
            try:
                clientsocket, _ = sock.accept()
            except socket.timeout:
                continue

            message = decode_message(receive_all(clientsocket))
            if message is None:
                continue
            LOGGER.debug("TCP recv\n%s", json.dumps(message, indent=2))
            try:
                handle_func(message)
            except (KeyError, TypeError, ValueError) as error:
                # Missing or wrongly typed fields: an invalid message, which
                # the spec says to ignore.  Keep listening.
                LOGGER.warning("Ignoring invalid message %s: %r",
                               message, error)


def udp_server(address, shutdown_event, handle_func):
    """Handle UDP messages on address until shutdown_event is set."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
        sock.settimeout(1)
        LOGGER.info("Listening on UDP port %s", address[1])

        while not shutdown_event.is_set():
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue

            message = decode_message(message_bytes)
            if message is None:
                continue
            try:
                handle_func(message)
            except (KeyError, TypeError, ValueError) as error:
                LOGGER.warning("Ignoring invalid message %s: %r",
                               message, error)
