#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Mock GCP KMS server for local testing.

import re
import json
import yaml
import uuid
import time
import base64
import urllib
import asyncio
import logging
import threading
import argparse
import signal
import datetime as dt
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from cryptography.fernet import Fernet

logger = logging.getLogger('fake_gcp_kms')

class GcpKmsVault:
    """
    Fake KMS vault
    """
    class AESKey:
        def __init__(self):
            self.def_version = None
            self.versions: dict[str,Fernet] = dict()
            self.users = set()
            self.create_version()

        def get_version(self, version = None) -> tuple[bytes, Fernet]:
            version = version or self.def_version
            return (uuid.UUID(version).bytes, self.versions.get(version))

        def create_version(self):
            version = uuid.uuid4().hex
            key = Fernet(Fernet.generate_key())
            self.versions[version] = key
            if not self.def_version:
                self.def_version = version;
            return version

        def encrypt(self, message: bytes, version: str = None) -> bytes:
            pfx, key = self.get_version(version)
            return pfx + key.encrypt(message)

        def decrypt(self, message: bytes) -> bytes:
            pfx = message[:16]
            msg = message[16:]
            _, key = self.get_version(uuid.UUID(bytes=pfx).hex)
            return key.decrypt(msg)

        def add_user(self, user: str):
            self.users.add(user)

        def has_user(self, user: str):
            return user in self.users

    def __init__(self):
        self.keys = dict()
        self.impersonators: dict[str, list] = dict()
        self.lock = threading.Lock()

    def add_impersonator(self, user: str, principal: str):
        self.impersonators.setdefault(user, list()).append(principal)

    def can_impersonate(self, user:str, principal: str):
        with self.lock:
            return principal in self.impersonators.get(user, [])

    def get_key(self, key_name: str) -> AESKey:
        with self.lock:
            return self.keys.get(key_name)
    
    def load_seed(self, seed):
        root = yaml.safe_load(seed)
        for proj,pd in root['projects'].items():
            for loc,ld in pd['locations'].items():
                for key_ring, krd in ld['keyRings'].items():
                    for ck, ckd in krd['cryptoKeys'].items():
                        kname = f"{proj}/{loc}/{key_ring}/{ck}"
                        key = self.AESKey()
                        for _ in ckd['versions'][1:]:
                            key.create_version()
                            logger.debug("Adding version to key %s", kname)
                        for user in ckd['users']:
                            key.add_user(user)
                            logger.debug("Adding user %s to key %s", user, kname)
                        logger.debug("Adding key %s", kname)
                        with self.lock:
                            self.keys[kname] = key
        for imp,principals in root.get('impersonators', {}).items():
            for p in principals:
                logger.debug("Adding impersonator %s -> %s", imp, p)
                self.add_impersonator(imp, p)

class GcpKmsRequestHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler

    Supported endpoints:
    - POST /v1/projects/{project}/locations/{location}/keyRings/{keyRing}/cryptoKeys/{keyName}:{action}";
    - POST /token (fake oauth)
    """
    keyop_expression = re.compile(r'^/v1/projects/([^/]+)/locations/([^/]+)/keyRings/([^/]+)/cryptoKeys/([^/]+):(\w+)$')
    oauth_path = "/token"
    impersonate_expression = re.compile(r'^/v1/projects/-/serviceAccounts/([^:]+):generateAccessToken$')

    TOKEN_DURATION = 86400  # 24 hours

    def __init__(self, vault: GcpKmsVault, *args, **kwargs):
        self.vault = vault
        super().__init__(*args, **kwargs)

    def extract_client_id(self):
        """Extract client ID from Authorization header if present"""
        auth_header = self.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            try:
                token = auth_header[7:]
                return base64.urlsafe_b64decode(token).decode()
            except Exception:
                pass
        return None

    def do_send_response(self, status_code, response):
        s = json.dumps(response).encode()
        self.send_response(status_code)
        self.send_header('Content-Type', "application/json")
        self.send_header('Content-Length', str(len(s)))
        self.end_headers()
        self.wfile.write(s)
        logger.debug("response %s: %s", status_code, response)

    def make_error_response(self, status, message):
        self.do_send_response(status, { "status": status, "message": message })

    def do_POST(self):
        logger.debug("Request %s: %s", self.path, self.headers)

        if self.path == self.oauth_path:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            form_data = urllib.parse.parse_qs(body)
            for param in ['grant_type', 'assertion']:
                if not param in form_data:
                    self.make_error_response(400, f"missing required parameter {param}")
                    return

            assertion=form_data['assertion'][0]
            # jwt. we don't care about whether it is valid or not (probably not)
            # just generate a bearer we can extract user from.
            # note: this is _not even close_ to what actual gcp gives back.
            # but this matters not for us; we just need a value than can be parsed.
            try:
                chunk = assertion.split('.')[1].encode()
                # make sure we have padding
                decoded = base64.b64decode(chunk + b'==')
                data = json.loads(decoded)
                email = data['iss']
                self.do_send_response(200, {
                    "token_type": "Bearer",
                    "expires_in": self.TOKEN_DURATION,
                    "access_token": base64.b64encode(email.encode()).decode()
                })
            except Exception as e:
                self.make_error_response(400, f"error parsing auth token {e}")
            return

        match = self.impersonate_expression.match(self.path)
        if match:
            principal = match.groups()[0]
            user = self.extract_client_id()
            if self.vault.can_impersonate(user, principal):
                self.do_send_response(200, {
                    "expireTime": (dt.datetime.now() + dt.timedelta(seconds=self.TOKEN_DURATION)).strftime('%FT%TZ'),
                    "accessToken": base64.b64encode(principal.encode()).decode()
                })
            else:
                self.make_error_response(400, f"User {user} cannot impersonate {principal}")
            return

        match = self.keyop_expression.match(self.path)
        if match:
            project, location, key_ring, key_name, op = match.groups()
            keyId = f"{project}/{location}/{key_ring}/{key_name}"
            key = self.vault.get_key(keyId)

            if not key:
                self.make_error_response(404, f"Key {key_name} not found in {project}/{location}/{key_ring}")
                return

            content_length = self.headers['Content-Length']
            data = self.rfile.read(int(content_length) if content_length else -1)
            user = self.extract_client_id()

            if not user:
                self.make_error_response(400, "Missing auth")
                return

            if not key.has_user(user):
                self.make_error_response(403, f"{user} cannot access {keyId}")
                return

            body = json.loads(data.decode())

            if op == 'encrypt':
                buf = key.encrypt(base64.b64decode(body['plaintext']))
                self.do_send_response(200, { 'ciphertext': base64.b64encode(buf).decode() })
                return 

            if op == 'decrypt':
                buf = key.decrypt(base64.b64decode(body['ciphertext']))
                self.do_send_response(200, { 'plaintext': base64.b64encode(buf).decode() })
                return

            self.make_error_response(400, f"Unsupported op: {op}")
            return

        self.make_error_response(404, "Invalid path")

class MockGcpKmsServer:
    def __init__(self, host, port):
        self.vault = GcpKmsVault()
        handler = partial(GcpKmsRequestHandler, self.vault)
        self.server = ThreadingHTTPServer((host, port), handler)
        self.server_thread = None
        self.server.request_queue_size = 10
        self.is_stopped = asyncio.Event()
        self.is_stopped.set()

    @property
    def server_address(self):
        return self.server.server_address

    def load_seed(self, seed: str):
        self.vault.load_seed(seed)

    async def start(self):
        if self.is_stopped.is_set():
            logger.info("Starting GCP KMS mock server on %s", self.server.server_address)
            loop = asyncio.get_running_loop()
            self.server_thread = loop.run_in_executor(None, self.server.serve_forever)
            self.is_stopped.clear()

    async def stop(self):
        if not self.is_stopped.is_set():
            logger.info("Stopping GCP KMS mock server")
            self.server.shutdown()
            self.server.server_close()
            await self.server_thread
            self.is_stopped.set()

    async def run(self):
        try:
            await self.start()
            await self.is_stopped.wait()
        except (Exception, asyncio.CancelledError) as e:
            logger.error("Server error: %s", e)
            await self.stop()


if __name__ == '__main__':
    async def run():
        parser = argparse.ArgumentParser(description="GCP KMS mock server")
        parser.add_argument('--host', default='127.0.0.1')
        parser.add_argument('--port', type=int, default=0)
        parser.add_argument("--seed", help="Seed file")
        parser.add_argument('--log-level', default=logging.INFO,
                            choices=logging.getLevelNamesMapping().keys(),
                            help="Set log level")

        args = parser.parse_args()
        logging.basicConfig(level=args.log_level, format='%(levelname)s  %(asctime)s - %(message)s')
        server = MockGcpKmsServer(args.host, args.port)

        if args.seed:
            with open(args.seed, encoding='utf-8') as f:
                server.load_seed(f)

        await server.start()
        signal.sigwait({signal.SIGINT, signal.SIGTERM})
        await server.stop()

    asyncio.run(run())