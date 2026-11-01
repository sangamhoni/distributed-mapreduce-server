"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import socket
import threading
import click
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)


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


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        self.host = host
        self.port = port

        # Every thread checks this flag; setting it tells them all to stop.
        self.shutdown_event = threading.Event()

        prefix = "mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            self.tmpdir = tmpdir

            # Heartbeats arrive over UDP in a background thread.
            heartbeat_thread = threading.Thread(target=self.listen_udp)
            heartbeat_thread.start()

            # Commands arrive over TCP in this (main) thread.  This call
            # blocks until shutdown.
            self.listen_tcp()

            # Don't leave the constructor until every thread has stopped.
            heartbeat_thread.join()
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def listen_tcp(self):
        """Accept TCP connections and handle one JSON message from each."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)
            LOGGER.info("Listening on TCP port %s", self.port)

            while not self.shutdown_event.is_set():
                try:
                    clientsocket, _ = sock.accept()
                except socket.timeout:
                    continue

                message = decode_message(receive_all(clientsocket))
                if message is None:
                    continue
                LOGGER.debug("TCP recv\n%s", json.dumps(message, indent=2))
                self.handle_message(message)

    def listen_udp(self):
        """Receive UDP heartbeat messages until shutdown."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)
            LOGGER.info("Listening on UDP port %s", self.port)

            while not self.shutdown_event.is_set():
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue

                message = decode_message(message_bytes)
                if message is None:
                    continue
                LOGGER.debug("UDP recv\n%s", json.dumps(message, indent=2))

    def handle_message(self, message):
        """Dispatch one TCP message based on its message_type."""
        message_type = message.get("message_type")
        if message_type == "shutdown":
            self.shutdown()
        else:
            LOGGER.warning("Unhandled message_type %s", message_type)

    def shutdown(self):
        """Stop the Manager: every thread's loop exits on its next check."""
        LOGGER.info("shutting down")
        self.shutdown_event.set()


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
