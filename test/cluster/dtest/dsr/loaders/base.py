import logging
import time
from threading import Event, Thread

from cassandra import ConsistencyLevel
from ccmlib.node import Node

from dsr.base.d_entity import DEntity
from dtest_class import Tester

logger = logging.getLogger(__name__)


class LoaderBase(DEntity, Thread):
    create_session_retry = 10
    _tester: Tester = None
    _target_node: Node = None
    _session = None
    _errors_aggregation_time = 100
    _errors_aggregation_count = 10

    consistency_level = ConsistencyLevel.QUORUM
    serial_consistency_level = None

    def __init__(self, **kwargs):
        self._errors = {}
        self._last_error_cleanup = time.time()
        Thread.__init__(self)
        DEntity.__init__(self, **kwargs)
        self._to_stop = Event()
        self._to_stop.clear()

    def factual_serial_consistency(self):
        raise NotImplementedError(f"Should be overridden for class {self.__class__.__name__}")

    def check_if_can_operate(self):
        raise NotImplementedError(f"Should be overridden for class {self.__class__.__name__}")

    def merge_result(self, output):
        raise NotImplementedError(f"Should be overridden for class {self.__class__.__name__}")

    def _create_session(self):
        raise NotImplementedError(f"Should be overridden for class {self.__class__.__name__}")

    def _run_workload(self):
        raise NotImplementedError(f"Should be overridden for class {self.__class__.__name__}")

    def _is_session_operational(self):
        if not self._session:
            return False
        if not self._errors_had_happened():
            return True
        return self._check_if_session_is_alive()

    def _check_if_session_is_alive(self):
        raise NotImplementedError(f"Should be overridden for class {self.__class__.__name__}")

    def _errors_had_happened(self):
        return len(self._errors) > 100

    def _cleanup_errors(self):
        for body, errors in self._errors.items():
            kwargs = errors[0]
            logger.debug(f"Following errors reported {len(errors)} for the last {self._errors_aggregation_time} seconds :" + body.format(**kwargs))
        self._errors.clear()

    def _publish_error(self, body: str, **kwargs):
        time_now = time.time()
        if self._errors_aggregation_time + self._last_error_cleanup < time_now:
            self._cleanup_errors()
        errors = self._errors.get(body, [])
        if not errors:
            self._errors[body] = errors
        errors.append(kwargs)
        if len(errors) < self._errors_aggregation_count:
            logger.debug(body.format(**kwargs))

    def run(self):
        if not self.check_if_can_operate():
            return
        while not self._to_stop.is_set():
            if not self._is_session_operational():
                for _ in range(self.create_session_retry):
                    self._session = self._create_session()
            self._run_workload()

    def stop(self, timeout=None):
        self._to_stop.set()
        try:
            self.join(timeout)
        except:
            pass
        self._cleanup_errors()

    def bind(self, tester: Tester, node: Node):
        self._tester = tester
        self._target_node = node


class NoopLoader(LoaderBase):
    def start(self):
        pass

    def stop(self, timeout=None):
        pass

    def merge_result(self, output):
        pass

    def check_if_can_operate(self):
        return True
