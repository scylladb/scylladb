#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from dataclasses import dataclass

import pytest

from test.pylib.artifact_registry import ArtifactRegistry


@dataclass(frozen=True)
class FakeSuite:
    suite_key: str = "fake-suite"


@pytest.mark.asyncio
async def test_cleanup_before_exit_runs_registered_artifacts() -> None:
    registry = ArtifactRegistry()
    suite = FakeSuite()
    events: list[str] = []

    async def suite_artifact() -> None:
        events.append("suite")

    async def exit_artifact() -> None:
        events.append("exit")

    registry.add_suite_artifact(suite, suite_artifact)
    registry.add_exit_artifact(suite, exit_artifact)

    await registry.cleanup_before_exit()

    assert events == ["suite", "exit"]
    assert registry.suite_artifacts == {}
    assert registry.exit_artifacts == {}
