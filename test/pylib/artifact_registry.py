#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from typing import Protocol
from typing import Callable, Coroutine, List, Dict, Optional
import asyncio
import logging
from test.pylib.util import MultiException

Artifact = Coroutine

class ArtifactCleanupException(MultiException):
    pass

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
        errors = []
        for artifacts in self.suite_artifacts.values():
            for artifact in artifacts:
                artifact.close()
            errors += list(filter(lambda e: isinstance(e,Exception),
                                  await asyncio.gather(*artifacts, return_exceptions=True)))
        self.suite_artifacts = {}
        for artifacts in self.exit_artifacts.values():
            errors += list(filter(lambda e: isinstance(e,Exception),
                                 await asyncio.gather(*artifacts, return_exceptions=True)))
        self.exit_artifacts = {}
        if len(errors) > 0:
            error_msg = "Cleaning up befor exit finished with errors"
            logging.error(error_msg)
            raise ArtifactCleanupException(error_msg,errors)
        logging.info("Done cleaning up before exit...")

    async def cleanup_after_suite(self, suite: Suite, failed: bool) -> None:
        """At the end of the suite, delete all suite artifacts, if the suite
        succeeds, and, in all cases, delete all exit artifacts produced by
        the suite. Executing exit artifacts right away is a good idea
        because it kills running processes and frees their resources
        early."""
        logging.info("Cleaning up after suite %s...", suite.suite_key)
        # Only drop suite artifacts if the suite executed successfully.
        errors = []
        if not failed and suite in self.suite_artifacts:
            errors += list(filter(lambda e: isinstance(e,Exception),
                                  await asyncio.gather(*self.suite_artifacts[suite],return_exceptions=True)))
            del self.suite_artifacts[suite]
        if suite in self.exit_artifacts:
            errors += list(filter(lambda e: isinstance(e,Exception),
                                  await asyncio.gather(*self.exit_artifacts[suite], return_exceptions=True)))
            del self.exit_artifacts[suite]
        if len(errors) > 0:
            error_msg = f"Cleanup for suite {suite} finised with errors"
            logging.error(error_msg)
            raise ArtifactCleanupException(error_msg, errors)
        logging.info("Done cleaning up after suite %s...", suite.suite_key)

    def add_suite_artifact(self, suite: Suite, artifact: Callable[[], Artifact]) -> None:
        self.suite_artifacts.setdefault(suite, []).append(artifact())

    def add_exit_artifact(self, suite: Optional[Suite], artifact: Callable[[], Artifact]):
        self.exit_artifacts.setdefault(suite, []).append(artifact())
