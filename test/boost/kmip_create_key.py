# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Helper script to create a KMIP key using the PyKMIP client.

import ssl
import sys
import argparse

from kmip import enums
from kmip.pie.client import ProxyKmipClient


def main():
    parser = argparse.ArgumentParser(description="KMIP Client - Create Key")
    parser.add_argument("config", help="Path to configuration file")
    args = parser.parse_args()
    config = args.config

    def fake_wrap_ssl(sock, keyfile=None, certfile=None,
                server_side=False, cert_reqs=ssl.CERT_NONE,
                ssl_version=ssl.PROTOCOL_TLS, ca_certs=None,
                do_handshake_on_connect=True,
                suppress_ragged_eofs=True,
                ciphers='ECDHE+AESGCM'):
        ctxt = ssl.SSLContext(protocol = ssl_version)
        ctxt.load_cert_chain(certfile=certfile, keyfile=keyfile)
        ctxt.verify_mode = cert_reqs
        ctxt.load_verify_locations(cafile=ca_certs)
        ctxt.set_ciphers(ciphers)
        return ctxt.wrap_socket(sock, server_side=server_side,
                                do_handshake_on_connect=do_handshake_on_connect,
                                suppress_ragged_eofs=suppress_ragged_eofs)

    # Monkey patch ssl.wrap_socket to let the PyKMIP client work with Python > 3.12.
    # Same trick as in kmip_wrapper.py.
    ssl.wrap_socket = fake_wrap_ssl

    with ProxyKmipClient(config_file=config) as client:
        key_id = client.create(
            algorithm=enums.CryptographicAlgorithm.AES,
            length=256
        )
        print(f"Created key with id: {key_id}")
        sys.stdout.flush()


if __name__ == "__main__":
    main()