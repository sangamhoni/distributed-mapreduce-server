"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import threading
import queue
import shutil
import collections
import time
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
import click
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)

# A Worker that misses more than 5 heartbeats (sent every 2s) is dead.
HEARTBEAT_TIMEOUT = 5 * 2

# Fields each incoming TCP message must have, and their types.  Messages
# that don't match are invalid and get ignored.
REQUIRED_FIELDS = {
    "shutdown": {},
    "register": {"worker_host": str, "worker_port": int},
    "finished": {"task_id": int, "worker_host": str, "worker_port": int},
    "new_manager_job": {
        "input_directory": str,
        "output_directory": str,
        "mapper_executable": str,
        "reducer_executable": str,
        "num_mappers": int,
        "num_reducers": int,
    },
}


def is_valid(message):
    """Return True if message has every field its type needs."""
    fields = REQUIRED_FIELDS.get(message.get("message_type"))
    if fields is None:
        return False
    return all(
        isinstance(message.get(name), kind) for name, kind in fields.items()
    )


@dataclass
class RemoteWorker:
    """The Manager's view of one registered Worker."""

    host: str
    port: int
    state: str = "ready"  # "ready", "busy", or "dead"
    # Task message it's working on, if busy (left out of the log output).
    task: Optional[dict] = field(default=None, repr=False)
    # When we last heard from it; registering counts as hearing from it.
    last_heartbeat: float = field(default_factory=time.time, repr=False)


@dataclass
class Stage:
    """Tasks of the running stage, shared by the job and main threads.

    The job thread hands out tasks and the main thread records finished
    ones, so both hold `condition` while touching this or a Worker's state.
    """

    condition: threading.Condition = field(
        default_factory=threading.Condition
    )
    pending: collections.deque = field(
        default_factory=collections.deque
    )  # not yet assigned to a Worker
    remaining: int = 0  # assigned or not, but not finished yet


def make_map_tasks(job, job_dir):
    """Split a job's input files round robin into new_map_task messages."""
    input_files = sorted(Path(job["input_directory"]).iterdir())
    num_mappers = job["num_mappers"]
    tasks = []
    for task_id in range(num_mappers):
        # Files task_id, task_id + num_mappers, task_id + 2*num_mappers, ...
        partition = input_files[task_id::num_mappers]
        tasks.append({
            "message_type": "new_map_task",
            "task_id": task_id,
            "input_paths": [str(path) for path in partition],
            "executable": job["mapper_executable"],
            "output_directory": str(job_dir),
            "num_partitions": job["num_reducers"],
        })
    return tasks


def make_reduce_tasks(job, job_dir):
    """Group the map output files by partition into new_reduce_task messages.

    Reduce task N gets every map task's partition-N file.
    """
    tasks = []
    for task_id in range(job["num_reducers"]):
        input_paths = sorted(job_dir.glob(f"maptask*-part{task_id:05d}"))
        tasks.append({
            "message_type": "new_reduce_task",
            "task_id": task_id,
            "executable": job["reducer_executable"],
            "input_paths": [str(path) for path in input_paths],
            "output_directory": job["output_directory"],
        })
    return tasks


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        self.address = (host, port)

        # Every thread checks this flag; setting it tells them all to stop.
        self.shutdown_event = threading.Event()

        # Registered Workers keyed by (host, port), in registration order.
        self.workers = mapreduce.utils.ThreadSafeOrderedDict()

        # Jobs waiting to run, oldest first.  The next job gets this id.
        self.job_queue = queue.Queue()
        self.next_job_id = 0

        # Task bookkeeping for the stage (map or reduce) that's running.
        self.stage = Stage()

        prefix = "mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            self.tmpdir = Path(tmpdir)

            # Heartbeats arrive over UDP in a background thread.
            heartbeat_thread = threading.Thread(
                target=mapreduce.utils.udp_server,
                args=(self.address, self.shutdown_event,
                      self.handle_heartbeat),
            )
            heartbeat_thread.start()

            # Jobs run one at a time in their own background thread.
            job_thread = threading.Thread(target=self.run_jobs)
            job_thread.start()

            # Another thread looks for Workers that stopped sending heartbeats.
            fault_thread = threading.Thread(target=self.check_heartbeats)
            fault_thread.start()

            # Commands arrive over TCP in this (main) thread.  This call
            # blocks until shutdown.
            try:
                mapreduce.utils.tcp_server(
                    self.address, self.shutdown_event, self.handle_message
                )
            finally:
                # Even if the server loop crashed, stop the other threads so
                # the process can exit instead of hanging.
                self.shutdown_event.set()

            # Don't leave the constructor until every thread has stopped.
            heartbeat_thread.join()
            job_thread.join()
            fault_thread.join()
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def handle_heartbeat(self, message):
        """Record that a registered, living Worker is still alive."""
        LOGGER.debug("UDP recv\n%s", json.dumps(message, indent=2))
        key = (message.get("worker_host"), message.get("worker_port"))
        with self.stage.condition:
            if key not in self.workers or self.workers[key].state == "dead":
                # The spec says to ignore these; a dead Worker must register
                # again to come back.
                LOGGER.debug("Ignoring heartbeat from %s", key)
                return
            self.workers[key].last_heartbeat = time.time()

    def check_heartbeats(self):
        """Mark Workers dead once they've been silent too long."""
        while not self.shutdown_event.is_set():
            now = time.time()
            with self.stage.condition:
                for worker in self.workers.values():
                    if worker.state != "dead" and \
                            now - worker.last_heartbeat > HEARTBEAT_TIMEOUT:
                        LOGGER.warning("Missed heartbeats from %s", worker)
                        self.mark_dead(worker)
            # Check about once a second; wake immediately on shutdown.
            self.shutdown_event.wait(1)

    def mark_dead(self, worker):
        """Stop using a Worker and put its unfinished task back in line.

        Dead Workers stay in self.workers, as the spec requires.
        """
        with self.stage.condition:
            worker.state = "dead"
            if worker.task is not None:
                LOGGER.info("Reassigning task %s", worker.task["task_id"])
                # Back of the line: the spec says hand out all of this stage's
                # new tasks before tasks from dead Workers.
                self.stage.pending.append(worker.task)
                worker.task = None
            self.stage.condition.notify_all()

    def handle_message(self, message):
        """Dispatch one TCP message based on its message_type."""
        if not is_valid(message):
            LOGGER.warning("Ignoring invalid message %s", message)
            return
        message_type = message["message_type"]
        if message_type == "shutdown":
            self.shutdown()
        elif message_type == "register":
            self.register_worker(message)
        elif message_type == "new_manager_job":
            self.new_job(message)
        elif message_type == "finished":
            self.task_finished(message)
        else:
            LOGGER.warning("Unhandled message_type %s", message_type)

    def register_worker(self, message):
        """Record a new Worker and acknowledge it."""
        host = message["worker_host"]
        port = message["worker_port"]
        with self.stage.condition:
            old = self.workers[(host, port)] \
                if (host, port) in self.workers else None
            if old is not None and old.task is not None:
                # It died and restarted before we noticed, so the task it
                # had is lost; run it again.
                self.mark_dead(old)
            worker = RemoteWorker(host, port)
            self.workers[(host, port)] = worker
            LOGGER.info("Registered Worker %s", worker)
            self.send_to_worker(worker, {"message_type": "register_ack"})
            # A job may be waiting for a free Worker; wake it up.
            self.stage.condition.notify_all()

    def task_finished(self, message):
        """Mark a Worker ready again after it finishes its task."""
        key = (message["worker_host"], message["worker_port"])
        with self.stage.condition:
            if key not in self.workers:
                return
            worker = self.workers[key]
            if worker.task is None or \
                    worker.task["task_id"] != message["task_id"]:
                return
            LOGGER.info("Finished task %s %s", message["task_id"], worker)
            worker.task = None
            worker.state = "ready"
            self.stage.remaining -= 1
            self.stage.condition.notify_all()

    def send_to_worker(self, worker, message):
        """Send a message to a Worker; mark it dead if it can't be reached.

        Return True if the message was sent.
        """
        try:
            mapreduce.utils.tcp_send((worker.host, worker.port), message)
        except OSError as error:
            # ConnectionRefusedError (the spec's case) or any other network
            # failure: the Worker can't be reached.
            LOGGER.warning("%r, marking dead %s", error, worker)
            self.mark_dead(worker)
            return False
        return True

    def new_job(self, message):
        """Give a submitted job an id and add it to the job queue."""
        job = dict(message)
        job["job_id"] = self.next_job_id
        self.next_job_id += 1
        self.job_queue.put(job)
        LOGGER.info("Queued job %s", job["job_id"])

    def run_jobs(self):
        """Run queued jobs one at a time until shutdown."""
        while not self.shutdown_event.is_set():
            try:
                job = self.job_queue.get(timeout=1)
            except queue.Empty:
                continue
            self.run_job(job)

    def run_job(self, job):
        """Run one MapReduce job to completion."""
        output_dir = Path(job["output_directory"])
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)
        LOGGER.info("Created %s", output_dir)

        job_dir = self.tmpdir / f"job-{job['job_id']:05d}"
        job_dir.mkdir()
        LOGGER.info("Created %s", job_dir)

        LOGGER.info("Begin Map Stage")
        self.run_stage(make_map_tasks(job, job_dir))
        if self.shutdown_event.is_set():
            return
        LOGGER.info("End Map Stage")

        LOGGER.info("Begin Reduce Stage")
        self.run_stage(make_reduce_tasks(job, job_dir))
        if self.shutdown_event.is_set():
            return
        LOGGER.info("End Reduce Stage")

        # Intermediate files aren't needed once the output is written.
        shutil.rmtree(job_dir)
        LOGGER.info("Removed %s", job_dir)
        LOGGER.info("Finished job %s. Output directory: %s",
                    job["job_id"], output_dir)

    def run_stage(self, tasks):
        """Give tasks to ready Workers; return when all have finished.

        Also return early if the Manager is shutting down.
        """
        with self.stage.condition:
            self.stage.pending = collections.deque(tasks)
            self.stage.remaining = len(tasks)
            while self.stage.remaining > 0 \
                    and not self.shutdown_event.is_set():
                worker = self.next_ready_worker()
                if self.stage.pending and worker is not None:
                    self.assign_task(worker, self.stage.pending.popleft())
                else:
                    # Sleep until a Worker registers or finishes.
                    self.stage.condition.wait(timeout=1)

    def next_ready_worker(self):
        """Return the earliest-registered ready Worker, or None."""
        for worker in self.workers.values():
            if worker.state == "ready":
                return worker
        return None

    def assign_task(self, worker, task):
        """Send a task to a Worker; put it back in line if that fails."""
        if self.send_to_worker(worker, task):
            worker.state = "busy"
            worker.task = task
            LOGGER.info("Assigned task %s to %s", task["task_id"], worker)
        else:
            self.stage.pending.appendleft(task)

    def shutdown(self):
        """Tell every living Worker to shut down, then stop the Manager."""
        LOGGER.info("shutting down")
        try:
            for worker in self.workers.values():
                if worker.state != "dead":
                    self.send_to_worker(worker, {"message_type": "shutdown"})
        finally:
            # Stop no matter what happened while forwarding.
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
