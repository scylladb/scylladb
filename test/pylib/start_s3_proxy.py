#!/usr/bin/python3
import argparse
import asyncio
import logging
import os
import signal
import time

from s3_proxy import S3ProxyServer


async def run():
    parser = argparse.ArgumentParser(description="Start S3 proxy server")
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9002)
    parser.add_argument('--log-level', default=logging.WARNING,
                        choices=logging.getLevelNamesMapping().keys(),
                        help="Set log level")
    # The S3 server has no well-known port: s3mock_server.py publishes the
    # container's port on a free one on the host and exports the pair for the
    # shell to pick up, so default to that and insist on being told otherwise.
    # These names are S3MockServer.ENV_{ADDRESS,PORT}, spelled out because this
    # script runs from its own directory rather than as part of the package.
    s3_host = os.environ.get('S3_SERVER_ADDRESS_FOR_TEST')
    s3_port = os.environ.get('S3_SERVER_PORT_FOR_TEST')
    s3_uri = f'http://{s3_host}:{s3_port}' if s3_host and s3_port else None
    parser.add_argument('--s3-uri', default=s3_uri, required=s3_uri is None,
                        help="URI of the S3 server to forward to"
                             " (default: $S3_SERVER_ADDRESS_FOR_TEST:$S3_SERVER_PORT_FOR_TEST)")
    parser.add_argument('--max-retries', type=int, default=5)
    parser.add_argument('--rnd-seed', type=int, default=int(time.time()))
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)
    server = S3ProxyServer(args.host, args.port, args.s3_uri, args.max_retries, args.rnd_seed,
                           logging.getLogger('s3-proxy'))

    print('Starting S3 proxy server')
    await server.start()
    signal.sigwait({signal.SIGINT, signal.SIGTERM})
    print('Stopping S3 proxy server')
    await server.stop()


if __name__ == '__main__':
    asyncio.run(run())
