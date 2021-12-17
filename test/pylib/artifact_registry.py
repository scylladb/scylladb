#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from typing import Protocol
from typing import TypeVar, Callable, Awaitable, Optional, List, Dict
import asyncio
import logging

Artifact = Awaitable


class Suite(Protocol):
    name: str


class ArtifactRegistry:
    """ A global to all tests registry of all external
    resources and artifacts, such as open ports, directories with temporary
    files or running auxiliary processes. Contains a map of all glboal
    resources, and as soon as the resource is taken by the test it is
    reprsented in the artifact registry. """

    def __init__(self) -> None:
        self.suite_artifacts: Dict[Suite, List[Artifact]] = {}
        self.exit_artifacts: Optional[List[Artifact]] = None

    async def cleanup_before_exit(self) -> None:
        logging.critical("Cleaning up before exit...")
        if self.exit_artifacts:
            await asyncio.gather(*self.exit_artifacts)
            self.exit_artifacts = None
        logging.critical("Done cleaning up before exit...")

    async def cleanup_after_suite(self, suite: Suite) -> None:
        logging.critical("Cleaning up after suite %s...", suite.name)
        if suite in self.suite_artifacts:
            await asyncio.gather(*self.suite_artifacts[suite])
            del self.suite_artifacts[suite]
        logging.critical("Done cleaning up after suite %s...", suite.name)

    def add_suite_artifact(self, suite: Suite, artifact: Callable[[], Artifact]) -> None:
        if suite in self.suite_artifacts:
            self.suite_artifacts[suite].append(artifact())
        else:
            self.suite_artifacts[suite] = [artifact()]

    def add_exit_artifact(self, artifact: Callable[[], Artifact]):
        if not self.exit_artifacts:
            self.exit_artifacts = []
        self.exit_artifacts.append(artifact())
