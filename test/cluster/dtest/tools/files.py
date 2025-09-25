#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import os
import random


def corrupt_file(file_path: str, chunk_size: int = 1024, percentage: float = 0.2) -> None:
    """
    Corrupts a file by overwriting random chunks of its content with random data.
    Args:
        file_path (str): The path to the file to be corrupted.
        chunk_size (int, optional): The size of each random data chunk to write, in bytes.
        percentage (float, optional): The percentage of the file to corrupt, represented as a float between 0 and 1.
    """
    with open(file_path, "r+b") as f:
        file_size = os.path.getsize(file_path)
        chunks = int(file_size * percentage / chunk_size) or 1
        for _ in range(chunks):
            random_position = random.randint(0, max(file_size - chunk_size, 0))
            f.seek(random_position)
            f.write(os.urandom(chunk_size))
