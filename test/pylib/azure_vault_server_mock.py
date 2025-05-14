#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

# Mock Azure Vault server for local testing and error injection.

import re
import os
import json
import uuid
import time
import base64
import urllib
import asyncio
import logging
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding

UNKNOWN_RESOURCE = r"""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"/>
<title>404 - File or directory not found.</title>
<style type="text/css">
<!--
body{margin:0;font-size:.7em;font-family:Verdana, Arial, Helvetica, sans-serif;background:#EEEEEE;}
fieldset{padding:0 15px 10px 15px;}
h1{font-size:2.4em;margin:0;color:#FFF;}
h2{font-size:1.7em;margin:0;color:#CC0000;}
h3{font-size:1.2em;margin:10px 0 0 0;color:#000000;}
#header{width:96%;margin:0 0 0 0;padding:6px 2% 6px 2%;font-family:"trebuchet MS", Verdana, sans-serif;color:#FFF;
background-color:#555555;}
#content{margin:0 0 0 2%;position:relative;}
.content-container{background:#FFF;width:96%;margin-top:8px;padding:10px;position:relative;}
-->
</style>
</head>
<body>
<div id="header"><h1>Server Error</h1></div>
<div id="content">
 <div class="content-container"><fieldset>
  <h2>404 - File or directory not found.</h2>
  <h3>The resource you are looking for might have been removed, had its name changed, or is temporarily unavailable.</h3>
 </fieldset></div>
</div>
</body>
</html>
"""

INVALID_CLIENT_ID = "mock-client-id-invalid"


class Error:
    def __init__(self, error_type: str, count: int):
        self.error_type = error_type
        self.count = count


class AzureVault:
    """
    Azure Key Vault service.

    Supported operations: wrapkey, unwrapkey
    Supported algorithms: RSA-OAEP-SHA256

    If the key is not found, it is created automatically.
    """
    class RSAKey:
        @staticmethod
        def _create_version():
            version = uuid.uuid4().hex
            private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
            public_key = private_key.public_key()
            return version, private_key, public_key

        def __init__(self):
            version, prvkey, pubkey = self._create_version()
            self.default = version
            self.versions = {version: (prvkey, pubkey)}

        def get_version(self, version = None):
            return self.versions.get(version or self.default)

        def create_version(self):
            version, prvkey, pubkey = self._create_version()
            self.versions[version] = (prvkey, pubkey)
            return version

        def encrypt(self, message: bytes, version: str = None) -> bytes:
            _, pubkey = self.get_version(version)
            return pubkey.encrypt(message, padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            ))

        def decrypt(self, message: bytes, version = None) -> bytes:
            prvkey, _ = self.get_version(version)
            return prvkey.decrypt(message, padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            ))

    INVALID_KEY = "nonexistentkey"

    ERROR_TEMPLATES = {
        'KeyNotFound': (404, {
            "error": {
                "code": "KeyNotFound",
                "message": "A key with (name/id) {key_name}/{key_version} was not found in this key vault. If you recently deleted this key you may be able to recover it using the correct recovery command. For help resolving this issue, please see https://go.microsoft.com/fwlink/?linkid=2125182"
            }
        }),
        'Forbidden': (403, {
            "error": {
                "code": "Forbidden",
                "message": "Caller is not authorized to perform action on resource."
            }
        }),
        'Unauthorized': (401, {
            "error": {
                "code": "Unauthorized",
                "message": "[TokenExpired] Error validating token: 'S2S12086'."
            }
        }),
        'Throttled': (429, {
            "error": {
                "code": "Throttled",
                "message": "Request was not processed because too many requests were received. Reason: VaultRequestTypeLimitReached"
            }
        }),
        'BadRequest': (400, {
            "error": {
                "code": "BadRequest",
                "message": "Property has invalid value\r\n"
            }
        })
    }

    def __init__(self):
        self.keys = {}
        self.error_config = None
        self.lock = threading.Lock()

    def get_or_create_key(self, key_name: str):
        with self.lock:
            if key_name not in self.keys:
                self.keys[key_name] = self.RSAKey()
            return self.keys[key_name]

    def get_key(self, key_name: str):
        with self.lock:
            return self.keys.get(key_name)

    def _error_response(self, error_type, **fmt_args):
        if error_type not in self.ERROR_TEMPLATES:
            raise ValueError(f"Unknown error type: {error_type}")
        status_code, template = self.ERROR_TEMPLATES[error_type]
        response = template.copy()
        if fmt_args:
            response['error'] = template['error'].copy()
            response['error']['message'] = template['error']['message'].format(**fmt_args)
        return status_code, response

    def wrapkey(self, data: str, key_name: str, key_version: str = None):
        try:
            if key_name == self.INVALID_KEY:
                return self._error_response('KeyNotFound',
                                            key_name=key_name,
                                            key_version=key_version or '')
            j = json.loads(data)
            alg, value = j['alg'], j['value']
            # TODO: Validate the algorithm, we only support RSA-OAEP-SHA256
            key = self.get_or_create_key(key_name)
            key_version = key_version or key.default

            with self.lock:
                if self.error_config is not None:
                    error = self.error_config
                    if error.count > 0:
                        error.count -= 1
                        return self._error_response(error.error_type,
                                                    key_name=key_name,
                                                    key_version=key_version)

            wrapped = key.encrypt(bytes(value, 'utf-8'), key_version)
            ret = base64.urlsafe_b64encode(wrapped).decode('utf-8')
            return (200, {
                "kid": f"https://mock-vault/keys/{key_name}/{key_version}",
                "value": ret
            })
        except Exception as e:
            return (400, {
                "error": {
                    "code": "BadRequest",
                    "message": f"Failed to wrapkey: {str(e)}"
                }
            })

    def unwrapkey(self, data: str, key_name: str, key_version: str = None):
        try:
            if key_name == self.INVALID_KEY:
                return self._error_response('KeyNotFound',
                                            key_name=key_name,
                                            key_version=key_version or '')
            j = json.loads(data)
            alg, value = j['alg'], j['value']
            # TODO: Validate the algorithm, we only support RSA-OAEP-SHA256
            key = self.get_key(key_name)
            version = key.get_version(key_version)

            with self.lock:
                if self.error_config is not None:
                    error = self.error_config
                    if error.count > 0:
                        error.count -= 1
                        return self._error_response(error.error_type,
                                                    key_name=key_name,
                                                    key_version=key_version)

            if not (key and version):
                return self._error_response('KeyNotFound', key_name=key_name, key_version=key_version or '')
            unwrapped = key.decrypt(base64.urlsafe_b64decode(value), key_version)
            return (200, {
                "kid": f"https://mock-vault/keys/{key_name}",
                "value": unwrapped.decode('utf-8')
            })
        except Exception as e:
            return (400, {
                "error": {
                    "code": "BadRequest",
                    "message": f"Failed to unwrapkey: {str(e)}"
                }
            })

    def set_error_config(self, error_type, count):
        with self.lock:
            self.error_config = Error(error_type, count)


class AzureIMDS:
    """
    Azure Instance Metadata Service (IMDS)

    Supports only token requests.
    The token is a very simple JWT, with no signature or validation.
    The generated tokens are cached and they are valid for 24 hours.
    """
    TOKEN_DURATION = 86400  # 24 hours

    ERROR_TEMPLATES = {
        'NotFound': (404, {
            'error': 'not_found',
            'error_description': 'IMDS endpoint is updating.',
        }),
        'InternalError': (500, {
            'error': 'unknown',
            'error_description': 'Failed to retrieve token from the Active directory. For details see logs in <file path>.',
        }),
        'Throttled': (429, {
            'error': 'throttled',
            'error_description': 'API Rate Limits have been exceeded.',
        }),
        'NoIdentity': (401, {
            'error': 'invalid_request',
            'error_description': 'Identity not found.', # E.g., no Managed Identity assigned to the VM
        }),
        'MultipleIdentities': (401, {
            'error': 'invalid_request',
            'error_description': 'Multiple user assigned identities exist, please specify the clientId / resourceId of the identity in the token request',
        }),
    }

    def __init__(self):
        self.token = None
        self.token_resource = None
        self.expires = 0
        self.error_config = None
        self.lock = threading.Lock()

    @staticmethod
    def base64url_encode(data: bytes) -> bytes:
        return base64.urlsafe_b64encode(data).rstrip(b'=')

    @staticmethod
    def generate_fake_jwt(resource: str, duration: int = TOKEN_DURATION):
        epoch = int(time.time())
        expires = epoch + duration
        header = {"alg": "none", "typ": "JWT"}
        payload = {
            "aud": resource,
            "iss": "https://sts.windows.net/mock-tenant-id/",
            "sub": "mock-client-id",
            "exp": expires,
            "nbf": epoch,
            "iat": epoch
        }

        jwt_header = AzureIMDS.base64url_encode(json.dumps(header).encode())
        jwt_payload = AzureIMDS.base64url_encode(json.dumps(payload).encode())

        token = jwt_header + b"." + jwt_payload + b"."
        return (token.decode(), expires)

    def expired(self):
        return time.time() >= self.expires

    def _error_response(self, error_type):
        if error_type not in self.ERROR_TEMPLATES:
            raise ValueError(f"Unknown error type: {error_type}")
        return self.ERROR_TEMPLATES[error_type]

    def get_access_token(self, resource: str):
        with self.lock:
            if self.error_config is not None:
                error = self.error_config
                if error.count > 0:
                    error.count -= 1
                    return self._error_response(error.error_type)
            if not self.token or self.token_resource != resource or self.expired():
                self.token, self.expires = self.generate_fake_jwt(resource)
                self.token_resource = resource
            return (200, {
                "access_token": self.token,
                "expires_in": str(int(self.expires - time.time())),
                "resource": resource,
                "token_type": "Bearer"
            })

    def set_error_config(self, error_type, count):
        with self.lock:
            self.error_config = Error(error_type, count)


class AzureEntraSTS:
    """
    Azure Entra Security Token Service (STS)

    Supports token requests with Service Principal credentials (secrets, certificates).
    Validates the request form, but not the actual values.
    The generated tokens are not cached, and are valid for 1 hour.
    """
    TOKEN_DURATION = 3600  # 1 hour

    # https://learn.microsoft.com/en-us/entra/identity-platform/reference-error-codes#handling-error-codes-in-your-application
    ERROR_TEMPLATES = {
        'InvalidRequest': (400, {
                'error': 'invalid_request',
                'error_description': "AADSTS900144: The request body must contain the following parameter: '{missing_param}'",
                'error_codes': [900144],
        }),
        'InvalidClient': (400, {
                'error': 'invalid_client',
                'error_description': "AADSTS7000216: 'client_assertion', 'client_secret' or 'request' is required for the 'client_credentials' grant type.",
                'error_codes': [7000216],
        }),
        'InvalidSecret': (401, {
                'error': 'invalid_client',
                'error_description': "AADSTS7000215: Invalid client secret is provided.",
                'error_codes': [7000215],
        }),
        'TemporarilyUnavailable': (503, {
                'error': 'temporarily_unavailable',
                'error_description': 'The server is temporarily too busy to handle the request.',
                'error_codes': [90006],
        }),
    }

    @staticmethod
    def base64url_encode(data: bytes) -> bytes:
        return base64.urlsafe_b64encode(data).rstrip(b'=')

    @staticmethod
    def generate_fake_jwt(client_id: str, tenant_id: str, resource: str, duration: int = TOKEN_DURATION):
        epoch = int(time.time())
        expires = epoch + duration
        header = {"alg": "none", "typ": "JWT"}
        payload = {
            "aud": resource,
            "iss": f"https://sts.windows.net/{tenant_id}/",
            "appid": client_id,
            "tid": tenant_id,
            "oid": "00000000-0000-0000-0000-000000000000",
            "sub": client_id,
            "exp": expires,
            "nbf": epoch,
            "iat": epoch
        }

        jwt_header = AzureEntraSTS.base64url_encode(json.dumps(header).encode())
        jwt_payload = AzureEntraSTS.base64url_encode(json.dumps(payload).encode())

        token = jwt_header + b"." + jwt_payload + b"."
        return token.decode()

    def __init__(self):
        self.error_config = None
        self.lock = threading.Lock()

    def _error_response(self, error_type, **kwargs):
        if error_type not in self.ERROR_TEMPLATES:
            raise ValueError(f"Unknown error type: {error_type}")
        status_code, template = self.ERROR_TEMPLATES[error_type]
        response = template.copy()
        if kwargs:
            response['error_description'] = response['error_description'].format(**kwargs)
        return status_code, response

    def get_access_token(self, tenant_id, form_data):
        return_error = False
        with self.lock:
            if self.error_config is not None:
                error = self.error_config
                if error.count > 0:
                    error.count -= 1
                    return_error = True
        if return_error:
            return self._error_response(error.error_type)
        # Just check that all the required parameters are provided.
        # Ignore their values.
        required_params = ['client_id', 'scope', 'grant_type']
        missing_params = [param for param in required_params if param not in form_data]
        if missing_params:
            return self._error_response('InvalidRequest', missing_params=missing_params[0])
        if 'client_secret' not in form_data and 'client_assertion' not in form_data:
            return self._error_response('InvalidClient')
        if 'client_assertion' in form_data and 'client_assertion_type' not in form_data:
            return self._error_response('InvalidRequest', missing_params='client_assertion_type')

        client_id=form_data['client_id'][0]
        resource = form_data['scope'][0].removesuffix('.default')

        token = self.generate_fake_jwt(client_id, tenant_id, resource)
        return (200, {
            "token_type": "Bearer",
            "expires_in": self.TOKEN_DURATION,
            "ext_expires_in": self.TOKEN_DURATION,
            "access_token": token
        })

    def set_error_config(self, error_type, count):
        with self.lock:
            self.error_config = Error(error_type, count)


class RequestHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler

    Supported endpoints:
    - POST /keys/{key_name}/{key_version}/{wrapkey, unwrapkey} (Vault)
    - GET  /metadata/identity/oauth2/token (IMDS)
    - POST /{tenant_id}/oauth2/v2.0/token (Entra STS)
    - POST /config/error?service={vault,imds,entra}&error_type={KeyNotFound,Forbidden,...}&repeat={count} (Error injection)
    """
    keyop_re = re.compile(r'^/keys/([^/]+)/([^/]*)/([^/?]+)(\?.*)?$')
    entra_token_re = re.compile(r'^/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/oauth2/v2\.0/token$')
    imds_token_path = '/metadata/identity/oauth2/token'
    error_path = "/config/error?"

    def __init__(self, vault, imds, entra, logger, *args, **kwargs):
        self.vault = vault
        self.imds = imds
        self.entra = entra
        self.logger = logger
        super().__init__(*args, **kwargs)

    def log_message(self, format, *args):
        if not self.logger.isEnabledFor(logging.DEBUG):
            return
        self.logger.debug("%s - - [%s] %s",
                         self.client_address[0],
                         self.log_date_time_string(),
                         format % args)

    def handle_one_request(self):
        # Check for TLS handshake (first byte is typically 0x16 in TLS)
        try:
            first_byte = self.rfile.peek(1)[0]
            if first_byte == 0x16:
                self.close_connection = True
                self.logger.error("TLS/HTTPS connection attempted. This server only supports plain HTTP.")
                return
        except (IndexError, AttributeError):
            pass
        super().handle_one_request()

    def _extract_client_id(self):
        """Extract client ID from Authorization header if present"""
        auth_header = self.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            try:
                token = auth_header[7:]
                parts = token.split('.')
                if len(parts) >= 2:
                    padding = '=' * (4 - len(parts[1]) % 4)
                    payload = json.loads(base64.urlsafe_b64decode(parts[1] + padding).decode('utf-8'))
                    return payload.get('appid')
            except Exception:
                pass
        return None

    def _send_response(self, status_code, mime_type, response):
        self.send_response(status_code)
        self.send_header('Content-Type', mime_type)
        self.send_header('Content-Length', str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def do_POST(self):
        # Error injection endpoint
        if self.path.startswith(self.error_path):
            get = lambda params, key : params[key][0] if key in params and params[key] else None
            parsed_url = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed_url.query)

            service = get(params, 'service')
            error_type = get(params, 'error_type')
            repeat = int(get(params, 'repeat') or '1')

            if None in (service, error_type):
                resp = {"error": {"code": "BadParameter", "message": "Query parameters 'service' and 'error_type' are required."}}
                self._send_response(400, 'application/json', json.dumps(resp).encode('utf-8'))
                return

            svc = getattr(self, service, None)
            if svc is None:
                resp = {"error": {"code": "BadParameter", "message": "Invalid value for 'service'. Must be one of: 'vault', 'imds', 'entra'."}}
                self._send_response(400, 'application/json', json.dumps(resp).encode('utf-8'))
                return

            svc.set_error_config(error_type, repeat)
            self.send_response(200)
            self.end_headers()
            return

        # Entra token request endpoint
        match = self.entra_token_re.match(self.path)
        if match:
            tenant_id = match.group(1)
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            form_data = urllib.parse.parse_qs(body)

            status, resp = self.entra.get_access_token(tenant_id, form_data)
            self._send_response(status, 'application/json', json.dumps(resp).encode('utf-8'))
            return

        # Vault key operations endpoint
        match = self.keyop_re.match(self.path)
        if match:
            key_name, key_version, operation, _ = match.groups()
            content_length = self.headers['Content-Length']
            data = self.rfile.read(int(content_length) if content_length else -1)

            if self._extract_client_id() == INVALID_CLIENT_ID:
                status, resp = self.vault._error_response('Forbidden')
                self._send_response(status, 'application/json', json.dumps(resp).encode('utf-8'))
                return

            if operation == 'wrapkey':
                status, resp = self.vault.wrapkey(data, key_name, key_version)
                self._send_response(status, 'application/json', json.dumps(resp).encode('utf-8'))
            elif operation == 'unwrapkey':
                status, resp = self.vault.unwrapkey(data, key_name, key_version)
                self._send_response(status, 'application/json', json.dumps(resp).encode('utf-8'))
            else:
                resp = {"error":{"code":"BadParameter","message":f"Method POST does not allow operation '{operation}'"}}
                self._send_response(400, 'application/json', json.dumps(resp).encode('utf-8'))
            return

        self._send_response(404, 'text/html', UNKNOWN_RESOURCE.encode('utf-8'))

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        if parsed_path.path == self.imds_token_path:
            # IMDS token request endpoint
            metadata_header = self.headers.get('Metadata')
            if metadata_header != 'true':
                resp = {'error': 'invalid_request', 'error_description': 'Metadata header missing or invalid.'}
                self._send_response(400, 'application/json', json.dumps(resp).encode('utf-8'))
                return
            query_params = urllib.parse.parse_qs(parsed_path.query)
            for param in ['api-version', 'resource']:
                if param not in query_params:
                    resp = {'error': 'invalid_request', 'error_description': f'Missing required parameter "{param}". Fix the request and retry.'}
                    self._send_response(400, 'application/json', json.dumps(resp).encode('utf-8'))
                    return
            status, resp = self.imds.get_access_token(query_params['resource'][0])
            self._send_response(status, 'application/json', json.dumps(resp).encode('utf-8'))
        else:
            self._send_response(404, 'text/plain', '')


class MockAzureVaultServer:
    def __init__(self, host, port, logger):
        self.logger = logger
        self.vault = AzureVault()
        self.imds = AzureIMDS()
        self.entra = AzureEntraSTS()
        handler = partial(RequestHandler, self.vault, self.imds, self.entra, self.logger)
        self.server = ThreadingHTTPServer((host, port), handler)
        self.server_thread = None
        self.server.request_queue_size = 10
        self.is_stopped = asyncio.Event()
        self.is_stopped.set()
        self.envs = {'MOCK_AZURE_VAULT_SERVER_PORT': f'{port}', 'MOCK_AZURE_VAULT_SERVER_HOST': f'{host}'}

    def _set_environ(self):
        for key, value in self.envs.items():
            os.environ[key] = value

    def _unset_environ(self):
        for key in self.envs.keys():
            del os.environ[key]

    def get_envs_settings(self):
        return self.envs

    async def start(self):
        if self.is_stopped.is_set():
            self.logger.info(f'Starting Azure Vault mock server on {self.server.server_address}')
            self._set_environ()
            loop = asyncio.get_running_loop()
            self.server_thread = loop.run_in_executor(None, self.server.serve_forever)
            self.is_stopped.clear()

    async def stop(self):
        if not self.is_stopped.is_set():
            self.logger.info(f'Stopping Azure Vault mock server')
            self._unset_environ()
            self.server.shutdown()
            self.server.server_close()
            await self.server_thread
            self.is_stopped.set()

    async def run(self):
        try:
            await self.start()
            await self.is_stopped.wait()
        except (Exception, asyncio.CancelledError) as e:
            self.logger.error(f'Server error: {e}')
            await self.stop()