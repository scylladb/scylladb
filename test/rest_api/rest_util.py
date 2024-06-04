import time
import pytest
import threading

from contextlib import contextmanager

# A utility function for creating a new temporary snapshot.
# If no keyspaces are given, a snapshot is taken over all keyspaces and tables.
# If no tables are given, a snapshot is taken over all tables in the keyspace.
# If no tag is given, a unique tag will be computed using the current time, in milliseconds.
# It can be used in a "with", as:
#   with new_test_snapshot(cql, tag, keyspace, [table(s)]) as snapshot:
# This is not a fixture - see those in conftest.py.
@contextmanager
def new_test_snapshot(rest_api, keyspaces=[], tables=[], tag=""):
    if not tag:
        tag = f"test_snapshot_{int(time.time() * 1000)}"
    params = { "tag": tag }
    if type(keyspaces) is str:
        params["kn"] = keyspaces
    else:
        params["kn"] = ",".join(keyspaces)
    if tables:
        if type(tables) is str:
            params["cf"] = tables
        else:
            params["cf"] = ",".join(tables)
    resp = rest_api.send("POST", "storage_service/snapshots", params)
    resp.raise_for_status()
    try:
        yield tag
    finally:
        resp = rest_api.send("DELETE", "storage_service/snapshots", params)
        resp.raise_for_status()

# Tries to inject an error via Scylla REST API. It only works in specific
# build modes (dev, debug, sanitize), so this function will trigger a test
# to be skipped if it cannot be executed.
@contextmanager
def scylla_inject_error(rest_api, err, one_shot=False):
    rest_api.send("POST", f"v2/error_injection/injection/{err}", {"one_shot": str(one_shot)})
    response = rest_api.send("GET", f"v2/error_injection/injection")
    assert response.ok
    print("Enabled error injections:", response.content.decode('utf-8'))
    if response.content.decode('utf-8') == "[]":
        pytest.skip("Error injection not enabled in Scylla - try compiling in dev/debug/sanitize mode")
    try:
        yield
    finally:
        print("Disabling error injection", err)
        response = rest_api.send("DELETE", f"v2/error_injection/injection/{err}")

@contextmanager
def new_test_module(rest_api):
    resp = rest_api.send("POST", f"task_manager_test/test_module")
    resp.raise_for_status()
    try:
        yield
    finally:
        resp = rest_api.send("GET", f"task_manager/list_module_tasks/test", { "internal": "true" })
        resp.raise_for_status()
        for task in resp.json():
            rest_api.send("DELETE", "task_manager_test/test_task", { "task_id": task["task_id"] })

        resp = rest_api.send("DELETE", f"task_manager_test/test_module")
        resp.raise_for_status()

@contextmanager
def new_test_task(rest_api, args):
    resp = rest_api.send("POST", "task_manager_test/test_task", args)
    resp.raise_for_status()
    task_id = resp.json()
    try:
        yield task_id
    finally:
        resp = rest_api.send("DELETE", "task_manager_test/test_task", { "task_id": task_id })

@contextmanager
def set_tmp_task_ttl(rest_api, seconds):
    resp = rest_api.send("POST", "task_manager/ttl", { "ttl" : seconds })
    resp.raise_for_status()
    old_ttl = resp.json()
    try:
        yield old_ttl
    finally:
        resp = rest_api.send("POST", "task_manager/ttl", { "ttl" : old_ttl })
        resp.raise_for_status()

# Unfortunately by default Python threads print their exceptions
# (e.g., assertion failures) but don't propagate them to the join(),
# so the overall test doesn't fail. The following Thread wrapper
# causes join() to rethrow the exception, so the test will fail.
class ThreadWrapper(threading.Thread):
    def run(self):
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exception = e
    def join(self, timeout=None):
        super().join(timeout)
        if hasattr(self, 'exception'):
            raise self.exception
        return self.ret
