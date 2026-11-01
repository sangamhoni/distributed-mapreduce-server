"""MapReduce framework Worker node."""
import os
import logging
import threading
import click
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)

# Seconds between heartbeats, from the spec.
HEARTBEAT_INTERVAL = 2


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )
        self.address = (host, port)
        self.manager_address = (manager_host, manager_port)

        # Every thread checks this flag; setting it tells them all to stop.
        self.shutdown_event = threading.Event()

        # Created once the Manager acknowledges our registration.
        self.heartbeat_thread = None

        # Messages from the Manager arrive over TCP in this (main) thread.
        # Register only once the socket is listening, so the register_ack
        # reply has somewhere to go.  This call blocks until shutdown.
        mapreduce.utils.tcp_server(
            self.address, self.shutdown_event, self.handle_message,
            on_listen=self.register,
        )

        # Don't leave the constructor until every thread has stopped.
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join()
        LOGGER.info("shutting down")

    def register(self):
        """Ask the Manager to add this Worker to its pool."""
        LOGGER.info("Connecting to Manager %s", self.manager_address)
        try:
            mapreduce.utils.tcp_send(self.manager_address, {
                "message_type": "register",
                "worker_host": self.address[0],
                "worker_port": self.address[1],
            })
        except ConnectionRefusedError:
            LOGGER.error("Manager isn't running; shutting down")
            self.shutdown_event.set()

    def handle_message(self, message):
        """Dispatch one TCP message from the Manager by message_type."""
        message_type = message.get("message_type")
        if message_type == "shutdown":
            self.shutdown_event.set()
        elif message_type == "register_ack":
            self.start_heartbeats()
        else:
            LOGGER.warning("Unhandled message_type %s", message_type)

    def start_heartbeats(self):
        """Start the heartbeat thread, unless it's already running."""
        if self.heartbeat_thread is not None:
            return
        LOGGER.info("Starting heartbeat thread")
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeats)
        self.heartbeat_thread.start()

    def send_heartbeats(self):
        """Tell the Manager we're alive every 2 seconds until shutdown."""
        message = {
            "message_type": "heartbeat",
            "worker_host": self.address[0],
            "worker_port": self.address[1],
        }
        while not self.shutdown_event.is_set():
            mapreduce.utils.udp_send(self.manager_address, message)
            # Sleep 2 seconds, but wake immediately on shutdown.
            self.shutdown_event.wait(HEARTBEAT_INTERVAL)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
