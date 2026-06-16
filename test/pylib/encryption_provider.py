#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import os
import asyncio
import logging
import threading
import yaml
import json

from enum import Enum
from functools import cached_property

from test import TEST_DIR
from test.pylib.dockerized_service import DockerizedServer
from test.pylib.kmip_wrapper import KMIPServerWrapper
from test.pylib.azure_vault_server_mock import MockAzureVaultServer
from test.pylib.gcp_kms_server_mock import MockGcpKmsServer

import boto3

class KeyProvider(Enum):
    """Enumeration of key providers in scylla"""
    # pylint: disable=invalid-name
    local = "LocalFileSystemKeyProviderFactory"
    replicated = "ReplicatedKeyProviderFactory"
    kmip = "KmipKeyProviderFactory"
    kms = "KmsKeyProviderFactory"
    azure = "AzureKeyProviderFactory"
    gcp = "GcpKeyProviderFactory"

class KeyProviderFactory:
    """Base class for provider factories"""
    def __init__(self, key_provider : KeyProvider, tmpdir):
        self.key_provider = key_provider
        self.system_key_location = os.path.join(tmpdir, "resources/system_keys")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        pass

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        # pylint: disable=unused-argument
        """Is cipher type supported by provider"""
        return True

    def require_restart(self):
        """Must servers be restarted on keyspace parameter changes?"""
        return False

    def configuration_parameters(self) -> dict[str, str]:
        """scylla.conf entries for provider"""
        return {"system_key_directory": self.system_key_location}

    def additional_cf_options(self) -> dict[str, str]:
        # pylint: disable=unused-argument
        """keyspace options"""
        if self.key_provider:
            return {"key_provider": self.key_provider.value}
        return {}

class LocalFileSystemKeyProviderFactory(KeyProviderFactory):
    """LocalFileSystemKeyProviderFactory proxy"""
    def __init__(self, tmpdir):
        super(LocalFileSystemKeyProviderFactory, self).__init__(KeyProvider.local, tmpdir)
        self.secret_file = os.path.join(tmpdir, "test/node1/conf/data_encryption_keys")

    def additional_cf_options(self) -> dict[str, str]:
        """scylla.conf entries for provider"""
        return super().additional_cf_options() | {"secret_key_file": self.secret_file}

class ReplicatedKeyProviderFactory(KeyProviderFactory):
    """ReplicatedKeyProviderFactory proxy"""
    def __init__(self, tmpdir, scylla_exe):
        super(ReplicatedKeyProviderFactory, self).__init__(KeyProvider.replicated, tmpdir)
        self.system_key_file_name = "system_key"
        self.scylla_exe = scylla_exe

    async def __aenter__(self):
        await super().__aenter__()
        args = ["local-file-key-generator", "generate", os.path.join(self.system_key_location, self.system_key_file_name)]
        proc = await asyncio.create_subprocess_exec(self.scylla_exe, *args, stderr=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f'Could not generate system key: {stderr.decode()}')
        return self

    def additional_cf_options(self):
        return super().additional_cf_options() | {"system_key": self.system_key_file_name}

class KmipKeyProviderFactory(KeyProviderFactory):
    """KmipKeyProviderFactory proxy"""
    def __init__(self, tmpdir):
        super(KmipKeyProviderFactory, self).__init__(KeyProvider.kmip, tmpdir)
        self.tmpdir = tmpdir
        self.kmip_server_wrapper = None
        self.kmip_host = "kmip_test"
        self.kmip_port = 0
        self.kmip_server = None
        self.kmip_thread = None
        resourcedir = os.path.join(TEST_DIR,  "pylib/resources")
        # TODO: we really need certs generated on test run
        self.certs = { "certfile": os.path.join(resourcedir, "scylla.crt"),
                      "keyfile": os.path.join(resourcedir, "scylla.key"),
                      "truststore": os.path.join(resourcedir, "scylla.crt")
                      }

    def configuration_parameters(self) -> dict[str, str]:
        """scylla.conf entries for provider"""
        options = {
            "hosts": "127.0.0.1:" + str(self.kmip_port),
            "certificate": self.certs["certfile"],
            "keyfile": self.certs["keyfile"],
            "truststore": self.certs["truststore"],
            "priority_string": "SECURE128:+RSA:-VERS-TLS1.0:-ECDHE-ECDSA",
        }
        return super().configuration_parameters() | { "kmip_hosts": { self.kmip_host: options } }

    def kmip_serve(self):
        """KMIP server loop"""
        self.kmip_server_wrapper.serve()

    async def __aenter__(self):
        assert os.path.exists(self.certs["certfile"])
        assert os.path.exists(self.certs["keyfile"])
        assert os.path.exists(self.certs["truststore"])
        kmiplog = logging.getLogger("kmip.server")
        kmiplog.handlers.clear()  # make pykmip shut up a bit. log will written to log file (setup in init below)

        self.kmip_server_wrapper = KMIPServerWrapper(
            hostname="127.0.0.1",
            config_path=None,
            certificate_path=self.certs["certfile"],
            policy_path=self.tmpdir.strpath,
            key_path=self.certs["keyfile"],
            ca_path=self.certs["truststore"],
            auth_suite="TLS1.2",
            database_path=':memory:',
            logging_level='DEBUG',
            log_path=os.path.join(self.tmpdir, "pykmip.log"),
            enable_tls_client_auth=False,
        )
        self.kmip_server_wrapper.start()
        self.kmip_port = self.kmip_server_wrapper.port
        assert len(kmiplog.handlers) == 1
        self.kmip_thread = threading.Thread(name="kmip server", target=self.kmip_serve, daemon=True)
        self.kmip_thread.start()
        return self

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        # pylint: disable=bare-except
        if self.kmip_server_wrapper is not None:
            self.kmip_server_wrapper.stop()
            self.kmip_server_wrapper = None
        if self.kmip_thread is not None:
            self.kmip_thread.join()
            self.kmip_thread = None

    def additional_cf_options(self):
        return super().additional_cf_options() | {"kmip_host": self.kmip_host}

    def require_restart(self):
        return True

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        # Our KMIP server is not configured to support this configuration.
        # Test fails with error: Invalid key data length 80 for RC2/CBC and kmip.
        # Decided (Roy) don't test it
        return not ("RC2" in cipher_algorithm and secret_key_strength == 80)

class KMSKeyProviderFactory(KeyProviderFactory):
    """KMSKeyProviderFactory proxy"""
    def __init__(self, tmpdir):
        super(KMSKeyProviderFactory, self).__init__(KeyProvider.kms, tmpdir)
        self.tmpdir = tmpdir
        self.master_key = "alias/Scylla-test"
        self.kms_host = "kms_test"
        self.endpoint_url = None
        self.server = None
        self.region = None
    
    async def __aenter__(self):
        master_key = os.getenv('KMS_KEY_ALIAS')
        aws_region = os.getenv('KMS_AWS_REGION')
 
        if master_key is None:
            self.server = DockerizedServer("docker.io/nsmithuk/local-kms:3", self.tmpdir,
                                           logfilenamebase="local-kms",
                                           success_string="Local KMS started on",
                                           failure_string="address already in use",
                                           port=8080
                                           )
            await self.server.start()
            self.endpoint_url = f'http://{self.server.host}:{self.server.port}'
            self.create_master_key()
        else:
            if aws_region is None:
                aws_region = 'us-east-1'
            self.region = aws_region
            self.master_key = master_key
        return self

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        if self.server is not None:
            await self.server.stop()
            self.server = None

    @cached_property
    def kms_client(self):
        """A boto client"""
        return boto3.client("kms", endpoint_url=self.endpoint_url, region_name="None")

    def create_master_key(self, alias_name:str = None):
        # pylint: disable=broad-exception-caught
        """(re-)create key"""
        # create master key
        response = self.kms_client.create_key(Description="dtest", Tags=[{"TagKey": "Name", "TagValue": "dtest"}])
        key_id = response["KeyMetadata"]["KeyId"]
        if alias_name is None:
            alias_name = self.master_key
        try:
            self.kms_client.delete_alias(AliasName=alias_name)
        except Exception:
            pass
        self.kms_client.create_alias(AliasName=alias_name, TargetKeyId=key_id)
        return key_id

    def configuration_parameters(self) -> dict[str, str]:
        """scylla.conf entries for provider"""
        options = {
            "master_key": self.master_key
        }
        if self.endpoint_url is not None:
            options['endpoint'] = self.endpoint_url
        if self.region is not None:
            options['aws_regions'] = self.region
        return super().configuration_parameters() | { "kms_hosts": { self.kms_host: options } }

    def additional_cf_options(self):
        return super().additional_cf_options() | {"kms_host": self.kms_host}

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        return secret_key_strength >= 128

    def require_restart(self):
        return True


class AzureKeyProviderFactory(KeyProviderFactory):
    """AzureKeyProviderFactory proxy"""
    def __init__(self, tmpdir):
        super(AzureKeyProviderFactory, self).__init__(KeyProvider.azure, tmpdir)
        self.tmpdir = tmpdir
        self.azure_host = "azure_test"
        self.azure_server = None
        self.host = None;
        self.port = 0

    def configuration_parameters(self) -> dict[str, str]:
        """scylla.conf entries for provider"""
        options = {
            "master_key": f"http://{self.host}:{self.port}/mock-key",
            "azure_tenant_id": "00000000-1111-2222-3333-444444444444",
            "azure_client_id": "mock-client-id",
            "azure_client_secret": "mock-client-secret",
            "azure_authority_host": f"http://{self.host}:{self.port}"
        }

        return super().configuration_parameters() | { "azure_hosts": { self.azure_host: options } }

    async def __aenter__(self):
        self.azure_server = MockAzureVaultServer("127.0.0.1", 0, logging.getLogger('azure-vault-server'))
        await self.azure_server.start()
        self.host = self.azure_server.server_address[0]
        self.port = self.azure_server.server_address[1]
        return self

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        if self.azure_server is not None:
            await self.azure_server.stop()
            self.azure_server = None

    def additional_cf_options(self):
        return super().additional_cf_options() | {"azure_host": self.azure_host}

    def require_restart(self):
        return True


class GcpKeyProviderFactory(KeyProviderFactory):
    gcp_project_id = "scylla-kms-test"
    gcp_location = "global"
    gcp_keyring = "test-ring"
    gcp_keyname = "test-key"

    """GcpKeyProviderFactory proxy"""
    def __init__(self, tmpdir):
        super(GcpKeyProviderFactory, self).__init__(KeyProvider.gcp, tmpdir)
        self.tmpdir = tmpdir
        self.gcp_server = None
        self.gcp_host = "gcp_test"
        self.gcp_port = 0
        self.gcp_credentials_file = os.path.join(tmpdir, "creds.json")
        self.gcp_server = None
        self.gcp_user = "user1@apa.org"

    def configuration_parameters(self) -> dict[str, str]:
        """scylla.conf entries for provider"""
        options = {
            self.gcp_host: {
                "master_key": f"{self.gcp_keyring}/{self.gcp_keyname}",
                "gcp_project_id": self.gcp_project_id,
                "gcp_location": self.gcp_location,
                "gcp_credentials_file": self.gcp_credentials_file,
                "endpoint": "http://127.0.0.1:" + str(self.gcp_port),
            }
        }
        return super().configuration_parameters() | { "gcp_hosts": options } 

    async def __aenter__(self):
        self.gcp_server = MockGcpKmsServer("127.0.0.1", 0)
        seed = { "projects":
                { self.gcp_project_id:
                 { "locations":
                  { self.gcp_location:
                   { "keyRings":
                    { self.gcp_keyring:
                     { "cryptoKeys":
                      { self.gcp_keyname:
                       { "purpose": 'ENCRYPT_DECRYPT',
                         "versions": [{}],
                         "users": [ self.gcp_user ]
                       }
                      }
                     }
                    }
                   }
                  }
                 }
                }
            }
        self.gcp_server.load_seed(yaml.safe_dump(seed))
        self.gcp_port = self.gcp_server.server_address[1]
        await self.gcp_server.start()

        with open(self.gcp_credentials_file, "w", encoding='utf-8') as f:
            creds = {
                "type": "service_account",
                "project_id": self.gcp_project_id,
                "private_key_id": "c68de079f19d430a742bf18fed675a00caac13a2",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDCZ+yap8UNnxeR\nM1Ho10jx6JnpXdktDrBhF/vK/ENYmjiIjV1RsULtqJDHDrVJhK+k7chiA64M70XT\nOP/ABlvoNqPmX90WWroQd4HbEPnXMEMJYBiKg1vUtsUImB8soxKxCp0uY3sn+6cx\n8LNjvmI4waCclTTPIoLeh9s8USu5gr/ZrBQUjUlKLqSdoQfw4Tt1XRY0z4NDT3iX\naIrFeuwrXvnHKLFJ9j+jkdZm5qtjtBDR5C0tQbzUeD0xX0XTrPmgF8yt0JgmqUxs\nfp4/yGAOpuFpj5w11mRo6e4Kq2xvlxRJaBqoyd/70EgccwwBQ4gAN187CYp7PSuG\nFdD4MvAdAgMBAAECggEACVewNbB5VlG+crJqLcvmzAVXHDFv5evuSwQ5jAQ6glAL\nBnjwsqPXqQ8wQfixeqJ/RGhO+HLf0uxOyTtUgxhrI0o47zHNMK1UgsUTfwEeWJqP\npiwxkbqFV8Ae0O5qlR0TIWH2ssuCGCZOXyaHoHP+SWb4vn2nJ4srieEyhoAKH2SV\nOLlhB80QUd1xqVB9D6N6Ee13JQyWbDTqZPd8rSHJ3EmR9qxZtpxdtTKobpGNoFlH\nOXL26LAUMJ+N4A0Z6/RX5i/HJax5k2lrguawRzWibj8JtoH3V7iqpDmtSShpZdY4\n888EMePBo5AN0v01UYpOUQwUuMrPMp0EVUTjnDvnoQKBgQDon5tPo+axMvBImlA6\nZNoI5dnws3x4wZKHbhgGryU4kW9iGHKNU8sTVkvja40bJMNaPT+7wtuB+nOuLNjX\nZNOM4wRcqBgXPQ6elgrBwY5Pf2TVmiqdjvNLNWaI+3lRNiDg6Gf6TRQJ/0wa9IIt\nwlAgenpS1lI4I67oehjEwTYw0QKBgQDV8SWOFoAjNkWsN8/mCxxQZi1YwLS+xJsz\nlDYBlAAxVuyGJXCnJ7Q7AkKbIy365ZPh4sfFnLbfT5LNt//UUP87rpfcVeVuwUnP\nGM2+1Umo2j5ur9edlca97fMywj7c+3lOe/LBTkMP9KgnOAhAqLrsrOzSH77ChLtB\nmePcIER9jQKBgAMbFmzCyHK3NmQRw150OEEEKJvBGblXBEjQnHuCXSHbNzx9DRJ7\n+usgLNU1e2XQYNdUmAQ+vsWGfYLm0GJX00c/RLCkAeZVh1twr2YU2nyPO95qN4Vx\nAiiP5vWPPfhqm5fFIpZB7zGO+gomF5La1E0KtZVjjSd4un4aGziNR9bxAoGAFaB/\n/GIX5/dXibZGpOmgnhwGH3+zhclYKxmjb/tnHZW86T6lqbAgzwpGc2pV/pPwpBgJ\nu9dAwUhI/dTI3sylUIIwxcxFGjId5PqL6euju5b8UrIh6MM4SQDh4dKzCiG9vIpZ\nGuNvchB4YyaN5wNnif9dHUyqOv2x9Eq7NwhoBA0CgYEA1TeeQvYZgRK42jT5yC/R\nuPS+Eb8IhpeWHU52T+1SgSUFYmgNAbE7n0nHVzYE64IvsPmVZLrLhhXADjRHICvK\nhTtML1lBustG7Z9Tu66EZdQkvnJDwmVSZqVr2FmoOlXVS/qj4Tcc5kWvVp5ogI3u\npF0cRpeqEwY4dSWhiCznRkA=\n-----END PRIVATE KEY-----\n",
                "client_email": self.gcp_user,
                "client_id": "100849414604266807639",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": f"http://127.0.0.1:{self.gcp_port}/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test1-262%40scylla-kms-test.iam.gserviceaccount.com",
                "universe_domain": "googleapis.com"
            }
            json.dump(creds, f)

        return self

    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        # pylint: disable=bare-except
        if self.gcp_server is not None:
            await self.gcp_server.stop()

    def additional_cf_options(self):
        return super().additional_cf_options() | {"gcp_host": self.gcp_host}

    def require_restart(self):
        return True


def make_key_provider_factory(provider: KeyProvider, tmpdir, scylla_exe):
    """Create key provider factory for enum"""
    res = None
    if provider == KeyProvider.local:
        res = LocalFileSystemKeyProviderFactory(tmpdir)
    elif provider == KeyProvider.replicated:
        res = ReplicatedKeyProviderFactory(tmpdir, scylla_exe)
    elif provider == KeyProvider.kmip:
        res = KmipKeyProviderFactory(tmpdir)
    elif provider == KeyProvider.kms:
        res = KMSKeyProviderFactory(tmpdir)
    elif provider == KeyProvider.azure:
        res = AzureKeyProviderFactory(tmpdir)
    elif provider == KeyProvider.gcp:
        res = GcpKeyProviderFactory(tmpdir)
    else:
        raise RuntimeError(f'Unknown key_provider: {provider}')

    return res
