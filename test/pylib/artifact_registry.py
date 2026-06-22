#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import logging
from collections.abc import Callable, Coroutine


Artifact = Coroutine


class ArtifactRegistry:
    """ A global to all tests registry of all external
    resources and artifacts, such as open ports, directories with temporary
    files or running auxiliary processes. Contains a map of all glboal
    resources, and as soon as the resource is taken by the test it is
    represented in the artifact registry. """

    exit_artifacts: list[Artifact]

    @classmethod
    def init(cls) -> None:
        cls.exit_artifacts = []

    @classmethod
    async def cleanup_before_exit(cls) -> None:
        logging.info("Cleaning up before exit...")
        await asyncio.gather(*cls.exit_artifacts, return_exceptions=True)
        cls.exit_artifacts = []
        logging.info("Done cleaning up before exit...")

    @classmethod
    def add_exit_artifact(cls, artifact: Callable[[], Artifact]):
        cls.exit_artifacts.append(artifact())
