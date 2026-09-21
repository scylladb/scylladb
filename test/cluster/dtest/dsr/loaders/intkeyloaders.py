from .base import ConsistencyLevel, Event, LoaderBase


class IntKeyLoader(LoaderBase):
    """
    This is the loader that is designed for cases where primary key is an
    integer It is designed to hit as many ranges as it can by having static
    gap between updated keys big enough.
    """

    # Needed to avoid updating same primary keys by different loaders
    _global_shift = 0
    step = 1000000  # The gap between updated primary keys
    max_value = 2147483647  # Maximum value of primary key
    insert = None  # INSERT INTO ks.test(k,v) VALUES (?, ?) IF NOT EXISTS
    update = None  # UPDATE ks.test SET v = ? WHERE k=? IF EXISTS

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._to_stop = Event()
        self._to_stop.clear()
        self._results = []
        self._base_value = -2147483648 + self._global_shift
        self.__class__._global_shift += 1
        self._current_idx = 0

    def factual_serial_consistency(self):
        if self.serial_consistency_level is not None:
            return self.serial_consistency_level
        if self.serial_consistency_level or " IF " in self.insert or " IF " in self.update:
            return ConsistencyLevel.SERIAL
        return None

    def check_if_can_operate(self):
        return self._target_node.is_running()

    def _check_if_session_is_alive(self):
        try:
            self._session.execute("DESCRIBE system.batchlog")
            return True
        except:
            return False

    def _create_session(self):
        create_session_params = {"consistency_level": self.consistency_level}
        if self.serial_consistency_level is not None:
            create_session_params["serial_consistency_level"] = self.serial_consistency_level
        try:
            session = self._tester.patient_cql_connection(self._target_node, **create_session_params)
            self._insert_stmt = session.prepare(self.insert)
            self._update_stmt = session.prepare(self.update)
            return session
        except:
            pass
        return None

    def _run_workload(self):
        db_idx = self._base_value + self.step * self._current_idx
        if db_idx > self.max_value:
            self._current_idx = 0
            db_idx = self._base_value + self.step * self._current_idx

        act = 0
        to_add = False
        if len(self._results) - 1 < self._current_idx:
            act = 1
            to_add = True
        elif self._results[self._current_idx] is None:
            act = 1
        if act == 1:
            try:
                self._session.execute(self._insert_stmt.bind((db_idx, 0)))
                if to_add:
                    self._results.append(0)
                else:
                    self._results[self._current_idx] = 0
            except Exception as exc:  # noqa: BLE001
                self._publish_error(f"<{self._target_node.name} failed to insert record with key {{db_idx}} due to the {{error}}>", db_idx=db_idx, error=str(exc))
                if to_add:
                    self._results.append(None)
            self._current_idx += 1
            return
        try:
            self._session.execute(self._update_stmt.bind((self._results[self._current_idx] + 1, db_idx)))
            self._results[self._current_idx] += 1
        except Exception as exc:  # noqa: BLE001
            self._publish_error(f"<{self._target_node.name} failed to update record with key {{db_idx}} due to the {{error}}>", db_idx=db_idx, error=str(exc))
        self._current_idx += 1

    def merge_result(self, output):
        for n, val in enumerate(self._results):
            if val is None:
                continue
            idx = self._base_value + n * self.step
            if idx >= self.max_value:
                l = 1
            output[idx] = val

    def check_validity(self):
        if self.insert is None:
            raise ValueError(f"{self.__class__.__name__}: insert CQL is required")
        if self.update is None:
            raise ValueError(f"{self.__class__.__name__}: update CQL is required")
