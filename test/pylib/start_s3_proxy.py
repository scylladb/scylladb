#!/usr/bin/python3
import argparse
import asyncio
import logging
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
    parser.add_argument('--minio-uri', default="http://127.0.0.1:9000")
    parser.add_argument('--max-retries', type=int, default=5)
    parser.add_argument('--rnd-seed', type=int, default=int(time.time()))
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)
    server = S3ProxyServer(args.host, args.port, args.minio_uri, args.max_retries, args.rnd_seed,
                           logging.getLogger('s3-proxy'))

    print('Starting S3 proxy server')
    await server.start()
    signal.sigwait({signal.SIGINT, signal.SIGTERM})
    print('Stopping S3 proxy server')
    await server.stop()


if __name__ == '__main__':
    asyncio.run(run())
