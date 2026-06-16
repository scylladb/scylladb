#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Protocol


Artifact = Coroutine


class Suite(Protocol):
    suite_key: str

    def __str__(self):
        return self.suite_key


class ArtifactRegistry:
    """ A global to all tests registry of all external
    resources and artifacts, such as open ports, directories with temporary
    files or running auxiliary processes. Contains a map of all glboal
    resources, and as soon as the resource is taken by the test it is
    represented in the artifact registry. """

    suite_artifacts: dict[Suite, list[Artifact]]
    exit_artifacts: dict[Suite | None, list[Artifact]]

    @classmethod
    def init(cls) -> None:
        cls.suite_artifacts = {}
        cls.exit_artifacts = {}

    @classmethod
    async def cleanup_before_exit(cls) -> None:
        logging.info("Cleaning up before exit...")
        for artifacts in cls.suite_artifacts.values():
            for artifact in artifacts:
                artifact.close()
            await asyncio.gather(*artifacts, return_exceptions=True)
        cls.suite_artifacts = {}
        for artifacts in cls.exit_artifacts.values():
            await asyncio.gather(*artifacts, return_exceptions=True)
        cls.exit_artifacts = {}
        logging.info("Done cleaning up before exit...")

    @classmethod
    def add_suite_artifact(cls, suite: Suite, artifact: Callable[[], Artifact]) -> None:
        cls.suite_artifacts.setdefault(suite, []).append(artifact())

    @classmethod
    def add_exit_artifact(cls, suite: Suite | None, artifact: Callable[[], Artifact]):
        cls.exit_artifacts.setdefault(suite, []).append(artifact())
