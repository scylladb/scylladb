#!/usr/bin/python3
#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# AWS KMS proxy server to inject server errors

import asyncio
import requests
import uuid

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from functools import partial
from test.pylib.s3_proxy import _block_all_cookies

class AWSKMSForwardingHandler(BaseHTTPRequestHandler):
    """
    Extremely simple AWS KMS proxy, existing only to inject 
    server errors
    """
    forward_timeout = (30, 300)
    hop_by_hop_headers = frozenset(('CONNECTION',
                                    'KEEP-ALIVE',
                                    'PROXY-AUTHENTICATE',
                                    'PROXY-AUTHORIZATION',
                                    'TE',
                                    'TRAILER',
                                    'TRANSFER-ENCODING',
                                    'UPGRADE'))

    def __init__(self, logger, dest_uri, generate_errors: asyncio.Event, *args, **kwargs):
        self.dest_uri = dest_uri
        self.logger = logger
        self.generate_errors = generate_errors
        self.session = requests.Session()
        self.session.cookies.set_policy(_block_all_cookies())
        adapter = requests.adapters.HTTPAdapter(pool_connections=8,
                                                pool_maxsize=32,
                                                pool_block=False)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        self.logger.info("Hej hej")
        super().__init__(*args, **kwargs)


    def respond_with_error(self, msg: str):
        self.logger.info("Generating 500 response %s", msg)
        self.send_response(500)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Connection', 'keep-alive')
        req_uuid = str(uuid.uuid4())
        response = f"""<?xml version="1.0" encoding="UTF-8"?>

                                <Error>
                                    <Code>InternalServerError</Code>
                                    <Message>{msg}</Message>
                                    <RequestId>{req_uuid}</RequestId>
                                    <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                                </Error>""".encode('utf-8')
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def process_request(self):
        if self.generate_errors.is_set():
            self.respond_with_error("We do errors now")
            return

        body = None
        content_length = self.headers['Content-Length']
        if content_length:
            body = self.rfile.read(int(content_length))

        target_url = self.dest_uri + self.path
        headers = {key: value for key, value in self.headers.items()}
        try:
            response = self.session.request(self.command, target_url, headers=headers, data=body, timeout=self.forward_timeout)
            self.logger.info("Response %s", response)
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.upper() != 'CONTENT-LENGTH' and key.upper() not in self.hop_by_hop_headers:
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)
        except requests.exceptions.RequestException as e:
            # Forwarding to the KMS server failed (e.g. connection reset while it is
            # under load from concurrent requests). Nothing has been written to the client
            # yet, so respond with a well-formed retryable error instead of letting the
            # client hang until it eventually observes a bare connection drop.
            self.logger.warning("Failed to forward request to %s: %s", target_url, e)
            self.respond_with_error(str(e))
            return

    def do_GET(self):
        self.process_request()

    def do_POST(self):
        self.process_request()

    def do_PUT(self):
        self.process_request()

    def do_DELETE(self):
        self.process_request()

    def do_HEAD(self):
        self.process_request()


# Proxy server to setup `ThreadingHTTPServer` instance with custom request handler (see above), managing requests state
# in the `self.req_states`, adding custom logger, etc. This server will be started automatically from `test.py`. In
# addition, it is possible just to start this server using another script - `start_s3_proxy.py` to run it locally to
# provide proxy between tests and the S3 server
class AWSKMSForwardingServer:
    def __init__(self, host: str, port: int, dest_uri: str, logger):
        self.logger = logger
        self.generate_errors = asyncio.Event()
        self.dest_uri = dest_uri
        handler = partial(AWSKMSForwardingHandler, logger, dest_uri, self.generate_errors)
        self.server = ThreadingHTTPServer((host, port), handler)
        self.server_thread = None
        self.server.request_queue_size = 1000
        self.server.timeout = 10000
        self.server.socket.settimeout(10000)
        self.server.socket.listen(1000)
        self.is_running = False

    def set_generate_errors(self, on:bool):
        if on:
            self.generate_errors.set()
        else:
            self.generate_errors.clear()

    async def start(self):
        if not self.is_running:
            self.logger.info('Starting AWS KMS proxy server on %s -> %s', self.server.server_address, self.dest_uri)
            loop = asyncio.get_running_loop()
            self.server_thread = loop.run_in_executor(None, self.server.serve_forever, 0.05)
            self.is_running = True

    async def stop(self):
        if self.is_running:
            self.logger.info('Stopping AWS KMS proxy server')
            self.server.shutdown()
            await self.server_thread
            self.is_running = False
