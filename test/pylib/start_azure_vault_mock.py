#!/usr/bin/env python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import signal
import logging
import asyncio
import argparse
from azure_vault_server_mock import MockAzureVaultServer


async def run():
    parser = argparse.ArgumentParser(description="Start Azure Vault mock server")
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=5467)
    parser.add_argument('--log-level', default=logging.WARNING,
                        choices=logging.getLevelNamesMapping().keys(),
                        help="Set log level")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level, format='%(levelname)s  %(asctime)s - %(message)s')
    server = MockAzureVaultServer(args.host, args.port, logging.getLogger('azure-vault-server'))

    print('Starting Azure Vault mock server')
    await server.start()
    signal.sigwait({signal.SIGINT, signal.SIGTERM})
    print('Stopping Azure Vault mock server')
    await server.stop()


if __name__ == '__main__':
    asyncio.run(run())