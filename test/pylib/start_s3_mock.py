#!/usr/bin/python3
import asyncio
import sys
import signal
import argparse
import logging
from s3_server_mock import MockS3Server


async def run():
    parser = argparse.ArgumentParser(description="Start S3 mock server")
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=2012)
    parser.add_argument('--log-level', default=logging.WARNING,
                        choices=logging.getLevelNamesMapping().keys(),
                        help="Set log level")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)
    server = MockS3Server(args.host, args.port, logging.getLogger('s3-server'))

    print('Starting S3 mock server')
    await server.start()
    signal.sigwait({signal.SIGINT, signal.SIGTERM})
    print('Stopping S3 mock server')
    await server.stop()


if __name__ == '__main__':
    asyncio.run(run())
