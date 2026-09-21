#!/usr/bin/python3
#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Start the 3rd party services (LDAP + toxiproxy, S3 mock, S3 proxy) that a
pytest session normally starts on its own, so they can be reused by several
runs.  Start them in a shell of their own:

    ./tools/toolchain/dbuild ./test/pylib/start_3rd_party_services.py

and source what they publish in the shell that runs pytest.  Under dbuild that
has to happen inside the container, which passes no environment through:

    ./tools/toolchain/dbuild sh -c '. testlog/3rd_party/services.env && pytest --no-3rd-party-services ...'

Stop them with Ctrl-C, or with:

    ./tools/toolchain/dbuild ./test/pylib/start_3rd_party_services.py --stop
"""

import argparse
import asyncio
import os
import pathlib
import shlex
import signal
import sys
from random import randint

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from test import TOP_SRC_DIR
from test.pylib.artifact_registry import ArtifactRegistry as artifacts
from test.pylib.runner import start_3rd_party_services


async def run(base: pathlib.Path) -> None:
    env_file, stop_fifo = base / 'services.env', base / 'services.stop'
    base.mkdir(parents=True, exist_ok=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    artifacts.init()
    try:
        old_environ = dict(os.environ)
        await start_3rd_party_services(tempdir_base=base, toxiproxy_byte_limit=randint(0, 2000))

        # --stop asks for the shutdown through this fifo rather than by signalling
        # a recorded pid: the launcher usually runs in a container of its own,
        # where a pid means nothing to the shell that wants to stop it, while the
        # state directory is shared.  Opening it read-only and non-blocking returns
        # at once and makes the fd readable as soon as anything is written to it.
        # It is in place before services.env, so a file to source means a launcher
        # that can be stopped.
        stop_fifo.unlink(missing_ok=True)
        os.mkfifo(stop_fifo)
        loop.add_reader(os.open(stop_fifo, os.O_RDONLY | os.O_NONBLOCK), stop.set)

        # The services publish their addresses and credentials by setting
        # environment variables; hand over whatever they changed to the shell
        # running pytest.
        env_file.write_text(''.join(f'export {key}={shlex.quote(value)}\n'
                                    for key, value in sorted(os.environ.items())
                                    if old_environ.get(key) != value))
        print(f'Services started, source {env_file} in the shell that runs'
              f' pytest --no-3rd-party-services (inside the container, under dbuild)')
        await stop.wait()
    finally:
        print('Stopping services')
        stop_fifo.unlink(missing_ok=True)
        env_file.unlink(missing_ok=True)
        await artifacts.cleanup_before_exit()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--tmpdir', default=str(TOP_SRC_DIR / 'testlog'),
                        help='Same as pytest --tmpdir; services live in its 3rd_party subdirectory')
    parser.add_argument('--stop', action='store_true', help='Stop an already running instance')
    args = parser.parse_args()

    base = pathlib.Path(args.tmpdir).absolute() / '3rd_party'
    if args.stop:
        stop_fifo = base / 'services.stop'
        try:
            # ENXIO here means the fifo is left over from a launcher that is gone.
            fifo_fd = os.open(stop_fifo, os.O_WRONLY | os.O_NONBLOCK)
        except OSError as e:
            sys.exit(f'No running services found ({stop_fifo}: {e.strerror})')
        os.write(fifo_fd, b'stop')
        os.close(fifo_fd)
        return
    asyncio.run(run(base))


if __name__ == '__main__':
    main()
