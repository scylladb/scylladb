#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from typing import Protocol
from typing import Callable, Coroutine, List, Dict, Optional
import asyncio
import logging

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

    def __init__(self) -> None:
        self.suite_artifacts: Dict[Suite, List[Artifact]] = {}
        self.exit_artifacts: Dict[Optional[Suite], List[Artifact]] = {}

    async def cleanup_before_exit(self) -> None:
        logging.info("Cleaning up before exit...")
        for artifacts in self.suite_artifacts.values():
            for artifact in artifacts:
                artifact.close()
            await asyncio.gather(*artifacts, return_exceptions=True)
        self.suite_artifacts = {}
        for artifacts in self.exit_artifacts.values():
            await asyncio.gather(*artifacts, return_exceptions=True)
        self.exit_artifacts = {}
        logging.info("Done cleaning up before exit...")

    async def cleanup_after_suite(self, suite: Suite, failed: bool) -> None:
        """At the end of the suite, delete all suite artifacts, if the suite
        succeeds, and, in all cases, delete all exit artifacts produced by
        the suite. Executing exit artifacts right away is a good idea
        because it kills running processes and frees their resources
        early."""
        logging.info("Cleaning up after suite %s...", suite.suite_key)
        # Only drop suite artifacts if the suite executed successfully.
        if not failed and suite in self.suite_artifacts:
            await asyncio.gather(*self.suite_artifacts[suite])
            del self.suite_artifacts[suite]
        if suite in self.exit_artifacts:
            await asyncio.gather(*self.exit_artifacts[suite])
            del self.exit_artifacts[suite]
        logging.info("Done cleaning up after suite %s...", suite.suite_key)

    def add_suite_artifact(self, suite: Suite, artifact: Callable[[], Artifact]) -> None:
        self.suite_artifacts.setdefault(suite, []).append(artifact())

    def add_exit_artifact(self, suite: Optional[Suite], artifact: Callable[[], Artifact]):
        self.exit_artifacts.setdefault(suite, []).append(artifact())
