#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import shutil
import socket

import pytest

from test.pylib.dockerized_service import DockerizedServer

pytestmark = pytest.mark.skipif(shutil.which("podman") is None and shutil.which("docker") is None,
                                reason="needs podman or docker")


async def test_start_detects_port_taken_after_probe(tmp_path):
    """
    Reproducer for SCYLLADB-5599.

    local-kms prints its start message before binding the port, and exits if the
    bind fails. On CI the nested containers run in the host network, so the port
    DockerizedServer probed can be taken by any socket before the container binds it.
    start() must then not hand out the dead port.

    Run the container in the host network, and take the probed port with a connected
    (non-listening) socket right before the first launch, as an outgoing connection
    would. The probe is still open at that point, so take it on another loopback
    address, which the probe does not see either. The next launches are left alone.
    """
    held = []

    def docker_args(host, port):
        if not held:
            listener = socket.socket()
            listener.bind(("127.0.0.2", 0))
            listener.listen()
            thief = socket.socket()
            thief.bind(("127.0.0.2", port))
            thief.connect(listener.getsockname())
            held.extend([listener, thief, listener.accept()[0]])
        return ["--network", "host", "-e", f"PORT={port}"]

    server = DockerizedServer("docker.io/nsmithuk/local-kms:3", tmp_path,
                              logfilenamebase="local-kms",
                              docker_args=docker_args,
                              success_string="Local KMS started on",
                              failure_string="address already in use")
    try:
        await server.start()
        # The first launch must have gone exactly as on CI: start message, then failed bind.
        # Without the fix start() returns on the start message, before the bind fails, so wait for it.
        stolen_port = held[1].getsockname()[1]
        logfile = tmp_path / f"local-kms-{stolen_port}.log"
        for _ in range(100):
            log = logfile.read_text()
            if "bind: address already in use" in log:
                break
            await asyncio.sleep(0.1)
        started = log.find("Local KMS started on")
        bind_failed = log.find("bind: address already in use")
        assert 0 <= started < bind_failed, f"first launch did not hit the taken port:\n{log}"
        try:
            _, writer = await asyncio.open_connection(server.host, server.port)
        except OSError as e:
            pytest.fail(f"start() returned port {server.port}, which does not serve: {e}")
        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()
        for s in held:
            s.close()
