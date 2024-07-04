#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import aiohttp
import aiohttp.web
import asyncio
import contextlib
import collections
import json
import logging
import requests
import sys
import traceback

from typing import Any, Callable, Dict


logger = logging.getLogger(__name__)


class approximate_value:
    """
    Allow matching query params with a non-exact match, allowing for a given
    difference tolerance (delta).
    """
    def __init__(self, value=None, delta=None):
        self._value = value
        self._delta = delta

    def __eq__(self, v):
        coerced_v = type(self._value)(v)
        return abs(self._value - coerced_v) <= self._delta

    def to_json(self):
        return {"__type__": "approximate_value", "value": self._value, "delta": self._delta}


class expected_request:
    ANY = -1  # allow for any number of requests (including no requests at all), similar to the `*` quantity in regexp
    ONE = 0  # exactly one request is allowed
    MULTIPLE = 1  # one or more request is allowed

    def __init__(self, method: str, path: str, params: dict = {}, multiple: int = ONE,
                 response: Dict[str, Any] = None, response_status: int = 200, hit: int = 0):
        self.method = method
        self.path = path.rstrip("/")
        self.params = params
        self.multiple = multiple
        self.response = response
        self.response_status = response_status
        self.hit = hit

    def as_json(self):
        def param_to_json(v):
            try:
                return v.to_json()
            except AttributeError:
                return v

        return {
                "method": self.method,
                "path": self.path,
                "multiple": self.multiple,
                "params": {k: param_to_json(v) for k, v in self.params.items()},
                "response": self.response,
                "response_status": self.response_status,
                "hit": self.hit}

    def __eq__(self, o):
        return self.method == o.method and self.path == o.path and self.params == o.params

    def __str__(self):
        return json.dumps(self.as_json())

    def exhausted(self):
        return ((self.multiple == self.ONE and self.hit > 0)
                or self.multiple == self.ANY
                or (self.multiple >= self.MULTIPLE and self.hit >= self.multiple))


def _make_param_value(value):
    if type(value) is dict and "__type__" in value:
        cls = globals()[value["__type__"]]
        del value["__type__"]
        return cls(**value)

    return value


def _make_expected_request(req_json):
    return expected_request(
            req_json["method"],
            req_json["path"],
            params={k: _make_param_value(v) for k, v in req_json.get("params", dict()).items()},
            multiple=req_json.get("multiple", expected_request.ONE),
            response=req_json.get("response"),
            response_status=req_json.get("response_status", 200),
            hit=req_json.get("hit", 0))


class rest_server():
    EXPECTED_REQUESTS_PATH = "__expected_requests__"
    UNEXPECTED_REQUESTS_PATH = "__unexpected_requests__"

    def __init__(self):
        self.expected_requests = collections.defaultdict(list)
        self.unexpected_requests = 0

    @staticmethod
    def _request_key(method, path):
        return f"{method}:{path.rstrip('/')}"

    async def get_expected_requests(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.json_response([r.as_json() for rl in self.expected_requests.values() for r in rl])

    async def get_unexpected_requests(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.json_response(self.unexpected_requests)

    async def post_expected_requests(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        payload = await request.json()
        for request in map(_make_expected_request, payload):
            self.expected_requests[self._request_key(request.method, request.path)].append(request)
        logger.info(f"expected_requests: {self.expected_requests}")
        return aiohttp.web.json_response({})

    async def delete_expected_requests(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        self.expected_requests.clear()
        self.unexpected_requests = 0
        return aiohttp.web.json_response({})

    async def handle_generic_request(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        request_key = self._request_key(request.method, request.path)

        try:
            expected_requests = self.expected_requests[request_key]
        except KeyError:
            self.unexpected_requests += 1
            return aiohttp.web.Response(status=404, text=f"Request {request_key} not found in expected requests")

        this_req = expected_request(request.method, request.path, params=dict(request.query))

        if len(expected_requests) == 0:
            self.unexpected_requests += 1
            logger.error(f"unexpected request, expected no request, got {this_req}")
            return aiohttp.web.Response(status=500, text=f"Expected no requests, got {this_req}")

        expected_req = None
        expected_req_index = None
        for i, req in enumerate(expected_requests):
            if this_req == req:
                expected_req = req
                expected_req_index = i
                break

        if expected_req is None:
            reqs = '\n'.join([str(r) for r in expected_requests])
            self.unexpected_requests += 1
            logger.error(f"unexpected request, request {this_req} matches none of the expected requests:\n{reqs}")
            return aiohttp.web.Response(status=500, text=f"Request {this_req} doesn't match any expected request")

        if expected_req.multiple == expected_request.ONE:
            del expected_requests[expected_req_index]
        else:
            expected_req.hit += 1

        if expected_req.response is None:
            logger.info(f"expected_request: {expected_req}, no response")
            return aiohttp.web.json_response({})
        else:
            logger.info(f"expected_request: {expected_req}, response: {expected_req.response}")
            return aiohttp.web.json_response(expected_req.response, status=expected_req.response_status)


async def run_server(ip, port):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    server = rest_server()
    app = aiohttp.web.Application()

    def wrap_handler(handler: Callable) -> Callable:
        async def catching_handler(request) -> aiohttp.web.Response:
            """Catch all exceptions and return them to the client.
               Without this, the client would get an 'Internal server error' message
               without any details. Thanks to this the test log shows the actual error.
            """
            try:
                ret = await handler(request)
                if ret is not None:
                    return ret
                return aiohttp.web.Response()
            except Exception as e:
                tb = traceback.format_exc()
                logger.error(f'Exception when executing {handler.__name__}: {e}\n{tb}')
                return aiohttp.web.Response(status=500, text=str(e))
        return catching_handler

    app.router.add_routes([
        aiohttp.web.get(f"/{server.EXPECTED_REQUESTS_PATH}", wrap_handler(server.get_expected_requests)),
        aiohttp.web.post(f"/{server.EXPECTED_REQUESTS_PATH}", wrap_handler(server.post_expected_requests)),
        aiohttp.web.delete(f"/{server.EXPECTED_REQUESTS_PATH}", wrap_handler(server.delete_expected_requests)),
        aiohttp.web.get(f"/{server.UNEXPECTED_REQUESTS_PATH}", wrap_handler(server.get_unexpected_requests)),
        # Register all required rest API paths.
        # Unfortunately, we have to register here all the different routes, used by tests.
        # Fortunately, aiohttp supports variable paths and with that, there is not that many paths to register.
        aiohttp.web.route("*", "/cache_service/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/cache_service/{part1}/{part2}/{part3}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/column_family/", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/column_family/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/column_family/{part1}/{part2}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/column_family/{part1}/{part2}/{part3}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/column_family/{part1}/{part2}/{part3}/{part4}",
                          wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/compaction_manager/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/compaction_manager/{part1}/{part2}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/failure_detector/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/gossiper/{part1}/{part2}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/messaging_service/{part1}/{part2}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/snitch/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/storage_proxy/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/storage_proxy/{part1}/{part2}/{part3}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/storage_service/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/storage_service/{part1}/", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/storage_service/{part1}/{part2}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/storage_service/{part1}/{part2}/{part3}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/stream_manager/", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/system/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/system/{part1}/{part2}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/task_manager/{part1}", wrap_handler(server.handle_generic_request)),
        aiohttp.web.route("*", "/task_manager/{part1}/{part2}", wrap_handler(server.handle_generic_request)),
    ])

    logger.info("start serving")

    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, ip, port)
    await site.start()

    try:
        while True:
            await asyncio.sleep(3600)  # sleep forever
    except asyncio.exceptions.CancelledError:
        pass

    logger.info("stopping")

    await runner.cleanup()


def get_expected_requests(server):
    """Get the expected requests list from the server.

    This will contain all the unconsumed expected request currently on the
    server. Can be used to check whether all expected requests arrived.

    Params:
    * server - resolved `rest_api_mock_server` fixture (see conftest.py).
    """
    ip, port = server
    r = requests.get(f"http://{ip}:{port}/{rest_server.EXPECTED_REQUESTS_PATH}")
    r.raise_for_status()
    try:
        return [_make_expected_request(r) for r in r.json()]
    except json.decoder.JSONDecodeError:
        logger.exception('unable to decode server response as JSON: %r', r)
        raise


def get_unexpected_requests(server):
    """Get the number of unexpeced requests from the server.

    Any requests which didn't match an expected request is unexpected.
    The amount of such requests is stored in a counter.
    This counter is reset when clear_expected_requests() is called.
    """
    ip, port = server
    r = requests.get(f"http://{ip}:{port}/{rest_server.UNEXPECTED_REQUESTS_PATH}")
    r.raise_for_status()
    return r.json()


def clear_expected_requests(server):
    """Clear the expected requests list on the server.

    Params:
    * server - resolved `rest_api_mock_server` fixture (see conftest.py).
    """
    ip, port = server
    r = requests.delete(f"http://{ip}:{port}/{rest_server.EXPECTED_REQUESTS_PATH}")
    r.raise_for_status()


def set_expected_requests(server, expected_requests):
    """Set the expected requests list on the server.

    Params:
    * server - resolved `rest_api_mock_server` fixture (see conftest.py).
    * requests - a list of request objects
    """
    ip, port = server
    payload = json.dumps([r.as_json() for r in expected_requests])
    r = requests.post(f"http://{ip}:{port}/{rest_server.EXPECTED_REQUESTS_PATH}", data=payload)
    r.raise_for_status()


@contextlib.contextmanager
def expected_requests_manager(server, expected_requests):
    clear_expected_requests(server)
    set_expected_requests(server, expected_requests)
    try:
        yield
    finally:
        clear_expected_requests(server)


if __name__ == '__main__':
    sys.exit(asyncio.run(run_server(sys.argv[1], int(sys.argv[2]))))
