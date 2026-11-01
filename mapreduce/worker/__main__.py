"""MapReduce framework Worker node."""
import os
import logging
import threading
import tempfile
import subprocess
import hashlib
import heapq
import shutil
import contextlib
from pathlib import Path
import click
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)

# Seconds between heartbeats, from the spec.
HEARTBEAT_INTERVAL = 2

# Bytes buffered per open file or pipe.  Set explicitly because newer Pythons
# default to 128 KiB, which adds up quickly with one file per partition.
BUFFER_SIZE = 8192


def partition_number(key, num_partitions):
    """Return which reducer handles key (same key -> same partition)."""
    hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
    keyhash = int(hexdigest, base=16)
    return keyhash % num_partitions


def map_into_partitions(task, outfiles):
    """Run the map executable on every input file.

    Stream its output line by line into outfiles[partition], so memory use
    stays O(1) no matter how much the mapper prints.
    """
    for input_path in task["input_paths"]:
        with open(input_path, encoding="utf-8") as infile:
            with subprocess.Popen(
                [task["executable"]],
                stdin=infile,
                stdout=subprocess.PIPE,
                text=True,
                bufsize=BUFFER_SIZE,
            ) as map_process:
                for line in map_process.stdout:
                    if not line.endswith("\n"):
                        # Last line without a newline; don't let it run
                        # into the next line written to the same file.
                        line += "\n"
                    key = line.partition("\t")[0]
                    outfiles[partition_number(key, len(outfiles))].write(line)
        LOGGER.info("Executed %s input=%s", task["executable"], input_path)


def run_map_task(task, tmpdir):
    """Map, partition, and sort into tmpdir; return the output paths."""
    task_id = task["task_id"]
    paths = [
        tmpdir / f"maptask{task_id:05d}-part{partition:05d}"
        for partition in range(task["num_partitions"])
    ]

    # One open output file per partition, all closed when the block ends.
    with contextlib.ExitStack() as stack:
        outfiles = [
            stack.enter_context(
                path.open("w", encoding="utf-8", buffering=BUFFER_SIZE)
            )
            for path in paths
        ]
        map_into_partitions(task, outfiles)

    # Sort in a separate process so this one never holds a whole file.
    for path in paths:
        subprocess.run(["sort", "-o", str(path), str(path)], check=True)
        LOGGER.info("Sorted %s", path)
    return paths


def run_reduce_task(task, tmpdir):
    """Merge the sorted inputs into the reducer; return the output path."""
    path = tmpdir / f"part-{task['task_id']:05d}"
    with contextlib.ExitStack() as stack:
        infiles = [
            stack.enter_context(open(
                input_path, encoding="utf-8", buffering=BUFFER_SIZE
            ))
            for input_path in task["input_paths"]
        ]
        outfile = stack.enter_context(path.open("w", encoding="utf-8"))
        with subprocess.Popen(
            [task["executable"]],
            text=True,
            stdin=subprocess.PIPE,
            stdout=outfile,
            bufsize=BUFFER_SIZE,
        ) as reduce_process:
            # heapq.merge reads one line at a time from each sorted file
            # and yields them in overall sorted order.
            for line in heapq.merge(*infiles):
                reduce_process.stdin.write(line)
    LOGGER.info("Executed %s", task["executable"])
    return [path]


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
        try:
            mapreduce.utils.tcp_server(
                self.address, self.shutdown_event, self.handle_message,
                on_listen=self.register,
            )
        finally:
            # Even if the server loop crashed, stop the heartbeat thread so
            # the process can exit instead of hanging.
            self.shutdown_event.set()

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
        elif message_type == "new_map_task":
            self.run_task(task=message, run_func=run_map_task)
        elif message_type == "new_reduce_task":
            self.run_task(task=message, run_func=run_reduce_task)
        else:
            LOGGER.warning("Unhandled message_type %s", message_type)

    def run_task(self, task, run_func):
        """Run a map or reduce task in a private tmpdir, then report back.

        run_func writes its output files into the tmpdir and returns their
        paths; we move them to the task's output directory.
        """
        task_id = task["task_id"]
        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created %s", tmpdir)
            output_dir = Path(task["output_directory"])
            for path in run_func(task, Path(tmpdir)):
                shutil.move(path, output_dir / path.name)
                LOGGER.info("Moved %s -> %s", path, output_dir / path.name)
        LOGGER.info("Removed %s", tmpdir)

        try:
            mapreduce.utils.tcp_send(self.manager_address, {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.address[0],
                "worker_port": self.address[1],
            })
        except ConnectionRefusedError:
            LOGGER.error("Can't reach Manager to report task %s", task_id)

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
