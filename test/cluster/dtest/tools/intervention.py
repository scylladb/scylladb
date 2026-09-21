import logging
import random
import time
from threading import Thread

logger = logging.getLogger(__name__)


class InterruptBootstrap(Thread):
    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        self.node.watch_log_for("Prepare completed")
        self.node.stop(gently=False)


class InterruptCompaction(Thread):
    """
    Interrupt compaction by killing a node as soon as
    the "Compacting" string is found in the log file
    for the table specified. This requires debug level
    logging in 2.1+ and expects debug information to be
    available in a file called "debug.log" unless a
    different name is passed in as a parameter.
    """

    def __init__(self, node, tablename, filename="debug.log", delay=0):
        Thread.__init__(self)
        self.node = node
        self.tablename = tablename
        self.filename = filename
        self.delay = delay
        self.mark = node.mark_log(filename=self.filename)

    def run(self):
        self.node.watch_log_for(f"Compacting(.*){self.tablename}", from_mark=self.mark, filename=self.filename)
        if self.delay > 0:
            random_delay = random.uniform(0, self.delay)
            logger.debug(f"Sleeping for {random_delay} seconds")
            time.sleep(random_delay)
        logger.debug(f"Killing node {self.node.address()}")
        self.node.stop(gently=False)


class KillOnBootstrap(Thread):
    def __init__(self, node):
        Thread.__init__(self)
        self.node = node

    def run(self):
        self.node.watch_log_for(r"Starting to bootstrap|raft topology: start streaming|raft_topology - start streaming")
        self.node.stop(gently=False)


class InterruptDecommission(Thread):
    """
    Interrupt decommission by killing a node as soon as
    the "DECOMMISSIONING: unbootstrap starts" or according to parameter string is found in the log file
    for the node specified.
    """

    def __init__(self, node, filename="system.log", search_for="DECOMMISSIONING: unbootstrap starts"):
        Thread.__init__(self)
        self.node = node
        self.filename = filename
        self.search_for = search_for
        self.mark = node.mark_log(filename=self.filename)

    def run(self):
        self.node.watch_log_for(self.search_for, from_mark=self.mark, filename=self.filename)
        logger.debug(f"Killing node {self.node.address()}")
        self.node.stop(gently=False)
