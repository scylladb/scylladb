#!/usr/bin/python3
#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

# Mock S3 server to inject errors for testing.

import os
import sys
import asyncio
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
import uuid
from enum import Enum
from functools import partial
from collections import OrderedDict

sys.path.insert(0, os.path.dirname(__file__))


class Policy(Enum):
    SUCCESS = 0
    RETRYABLE_FAILURE = 1
    NONRETRYABLE_FAILURE = 2
    NEVERENDING_RETRYABLE_FAILURE = 3


class LRUCache:
    lock = threading.Lock()

    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str) -> Policy:
        with self.lock:
            if key not in self.cache:
                return Policy.SUCCESS
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: Policy) -> None:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)


# Simple HTTP handler for testing (at this moment) and injecting errors for S3 multipart uploads. It can be easily
# extended to support whatever S3 call is needed by adding any logic to the `do_*` handlers. Also one can extend this
# handler to process additional HTTP methods, for example HEAD.
# In case one have to keep some sort of tracking or state there is shared LRUCache maintained by the MockS3Server class
# (see below). At this moment this LRU is used to track failure policies for the test case by tracking it using the S3
# object name. One can extend it to whatever key to accommodate new needs.
class InjectingHandler(BaseHTTPRequestHandler):

    def __init__(self, policies, logger, *args, **kwargs):
        self.policies = policies
        self.logger = logger
        super().__init__(*args, **kwargs)
        self.close_connection = False

    def log_error(self, format, *args):
        if self.logger:
            self.logger.info("%s - - [%s] %s\n" %
                             (self.client_address[0],
                              self.log_date_time_string(),
                              format % args))
        else:
            sys.stderr.write("%s - - [%s] %s\n" %
                             (self.address_string(),
                              self.log_date_time_string(),
                              format % args))

    def log_message(self, format, *args):
        # Just don't be too verbose
        if not self.logger:
            sys.stderr.write("%s - - [%s] %s\n" %
                             (self.address_string(),
                              self.log_date_time_string(),
                              format % args))

    def parsed_qs(self):
        parsed_url = urlparse(self.path)
        query_components = parse_qs(parsed_url.query)
        for key in parsed_url.query.split('&'):
            if '=' not in key:
                query_components[key] = ['']
        return query_components

    def do_GET(self):
        content_length = self.headers['Content-Length']
        if content_length:
            data = self.rfile.read(int(content_length))
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Connection', 'keep-alive')
        if urlparse(self.path).path == '/latest/meta-data/iam/security-credentials/':
            response = "scylla_test_role".encode('utf-8')
        elif urlparse(self.path).path == '/latest/meta-data/iam/security-credentials/scylla_test_role' and self.headers[
            'x-aws-ec2-metadata-token'] == "VERYLONGSECURITYTOKEN":
            response = """{
  "Code" : "Success",
  "LastUpdated" : "2012-04-26T16:39:16Z",
  "Type" : "AWS-HMAC",
  "AccessKeyId" : "INSTANCE_FROFILE_EXAMPLE_ACCESS_KEY_ID",
  "SecretAccessKey" : "INSTANCE_FROFILE_EXAMPLE_SECRET_ACCESS_KEY",
  "Token" : "BBBBBBBBBBBBBBBBBBBBBINSTANCE_FROFILE_SESSIONTOKENBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB==",
  "Expiration" : "2017-05-17T15:09:54Z"
}""".encode('utf-8')
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    # Processes POST method, see `build_POST_reponse` for details
    def do_POST(self):
        content_length = self.headers['Content-Length']
        if content_length:
            put_data = self.rfile.read(int(content_length))
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Connection', 'keep-alive')
        response = self.build_POST_reponse(self.parsed_qs(), urlparse(self.path).path).encode('utf-8')
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    # Processes PUT method, at this moment it serves for providing response for S3 upload part method, also it used to
    # set values to the LRU cache entries by calling PUT with Key and Policy on the query string
    def do_PUT(self):
        content_length = self.headers['Content-Length']
        if content_length:
            put_data = self.rfile.read(int(content_length))
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Connection', 'keep-alive')
        query_components = self.parsed_qs()
        if 'Key' in query_components and 'Policy' in query_components:
            self.policies.put(query_components['Key'][0], Policy(int(query_components['Policy'][0])))
        elif 'uploadId' in query_components and 'partNumber' in query_components:
            self.send_header('ETag', "SomeTag_" + query_components.get("partNumber")[0])
        elif urlparse(self.path).path == '/latest/api/token':
            assert self.headers["x-aws-ec2-metadata-token-ttl-seconds"] is not None
            response_body = "VERYLONGSECURITYTOKEN".encode('utf-8')
            self.send_header('Content-Length', str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)
            return
        else:
            self.send_header('ETag', "SomeTag")

        if self.headers['x-amz-copy-source']:
            response_body = """<CopyPartResult>
                                    <LastModified>2011-04-11T20:34:56.000Z</LastModified>
                                    <ETag>"9b2cf535f27731c974343645a3985328"</ETag>
                               </CopyPartResult>""".encode('utf-8')
            self.send_header('Content-Length', str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)
            return

        self.send_header('Content-Length', '0')
        self.end_headers()

    # Processes DELETE method, does nothing except providing in the response expected headers and response code
    def do_DELETE(self):
        query_components = self.parsed_qs()
        if 'uploadId' in query_components and self.policies.get(
                urlparse(self.path).path) == Policy.NONRETRYABLE_FAILURE:
            self.send_response(404)
        else:
            self.send_response(204)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', '0')
        self.send_header('Connection', 'keep-alive')
        self.end_headers()

    # This function builds the XML response to satisfy the request type, for example `InitiateMultipartUpload` or
    # `CompleteMultipartUpload`, and failure policy. For example, when the policy is `RETRYABLE_FAILURE` it will
    # transmit the erroneous response for the first time and then switch the policy to `SUCCESS`. One can extend this
    # mechanism by adding additional policies or rewriting the response body logic
    def build_POST_reponse(self, query, path):
        if 'RoleArn' in query:
            return """<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
    <AssumeRoleResult>
        <SourceIdentity>Alice</SourceIdentity>
        <AssumedRoleUser>
            <Arn>arn:aws:sts::123456789012:assumed-role/demo/TestAR</Arn>
            <AssumedRoleId>ARO123EXAMPLE123:TestAR</AssumedRoleId>
        </AssumedRoleUser>
        <Credentials>
            <AccessKeyId>STS_EXAMPLE_ACCESS_KEY_ID</AccessKeyId>
            <SecretAccessKey>STS_EXAMPLE_SECRET_ACCESS_KEY</SecretAccessKey>
            <SessionToken>
                AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                BBBBBBBBBBBBBBBBBBBBBSTS_SESSIONTOKENBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
                CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC
                DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD
                EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE==
            </SessionToken>
            <Expiration>2019-11-09T13:34:41Z</Expiration>
        </Credentials>
        <PackedPolicySize>6</PackedPolicySize>
    </AssumeRoleResult>
    <ResponseMetadata>
        <RequestId>c6104cbe-af31-11e0-8154-cbc7ccf896c7</RequestId>
    </ResponseMetadata>
</AssumeRoleResponse>"""
        if 'uploads' in query:
            req_uuid = str(uuid.uuid4())
            return f"""<InitiateMultipartUploadResult>
                            <Bucket>bucket</Bucket>
                            <Key>key</Key>
                            <UploadId>{req_uuid}</UploadId>
                        </InitiateMultipartUploadResult>"""
        if 'uploadId' in query:
            match self.policies.get(path):
                case Policy.SUCCESS:
                    return """<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                                    <Location>http://Example-Bucket.s3.Region.amazonaws.com/Example-Object</Location>
                                    <Bucket>Example-Bucket</Bucket>
                                    <Key>Example-Object</Key>
                                    <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
                                </CompleteMultipartUploadResult>"""
                case Policy.RETRYABLE_FAILURE | Policy.NEVERENDING_RETRYABLE_FAILURE:
                    if self.policies.get(path) == Policy.RETRYABLE_FAILURE:
                        # should succeed on retry
                        self.policies.put(path, Policy.SUCCESS)
                    return """<?xml version="1.0" encoding="UTF-8"?>

                                <Error>
                                    <Code>InternalError</Code>
                                    <Message>We encountered an internal error. Please try again.</Message>
                                    <RequestId>656c76696e6727732072657175657374</RequestId>
                                    <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                                </Error>"""
                case Policy.NONRETRYABLE_FAILURE:
                    return """<?xml version="1.0" encoding="UTF-8"?>

                                <Error>
                                    <Code>InvalidAction</Code>
                                    <Message>Something went terribly wrong</Message>
                                    <RequestId>656c76696e6727732072657175657374</RequestId>
                                    <HostId>Uuag1LuByRx9e6j5Onimru9pO4ZVKnJ2Qz7/C1NPcfTWAtRPfTaOFg==</HostId>
                                </Error>"""
                case _:
                    raise ValueError("Unknown policy")


# Mock server to setup `ThreadingHTTPServer` instance with custom request handler (see above), managing requests state
# in the `self.req_states`, adding custom logger, etc. This server will be started automatically from `test.py` once the
# pytest kicks in. In addition, it is possible just to start this server using another script - `start_mock.py` to
# run it locally to provide mocking functionality for tests executed during development process
class MockS3Server:
    def __init__(self, host, port, logger):
        self.req_states = LRUCache(10000)
        handler = partial(InjectingHandler, self.req_states, logger)
        self.server = ThreadingHTTPServer((host, port), handler)
        self.server_thread = None
        self.server.request_queue_size = 1000
        self.server.timeout = 10000
        self.server.socket.settimeout(10000)
        self.server.socket.listen(1000)
        self.logger = logger
        self.is_running = False
        self.envs = {'MOCK_S3_SERVER_PORT': f'{port}', 'MOCK_S3_SERVER_HOST': f'{host}'}

    def _set_environ(self):
        for key, value in self.envs.items():
            os.environ[key] = value

    def _unset_environ(self):
        for key in self.envs.keys():
            del os.environ[key]

    def get_envs_settings(self):
        return self.envs

    async def start(self):
        if not self.is_running:
            self.logger.info('Starting S3 mock server on %s', self.server.server_address)
            self._set_environ()
            loop = asyncio.get_running_loop()
            self.server_thread = loop.run_in_executor(None, self.server.serve_forever)
            self.is_running = True

    async def stop(self):
        if self.is_running:
            self.logger.info('Stopping S3 mock server')
            self._unset_environ()
            self.server.shutdown()
            await self.server_thread
            self.is_running = False

    async def run(self):
        try:
            await self.start()
            while self.is_running:
                await asyncio.sleep(1)
        except Exception as e:
            self.logger.error("Server error: %s", e)
            await self.stop()
