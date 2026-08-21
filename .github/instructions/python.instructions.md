---
applyTo: "**/*.py"
---

# Python Guidelines

**Important:** Match existing code style. Some directories (like `test/cqlpy` and `test/alternator`) prefer simplicity over type hints and docstrings.

## Style
- Match style of the file and directory you are editing; fall back to PEP 8 if unclear
- Use type hints for function signatures (unless directory style omits them)
- Use f-strings for formatting
- Line length: 160 characters max
- 4 spaces for indentation

## Imports
Order: standard library, third-party, local imports
```python
import os
import sys

import pytest
from cassandra.cluster import Cluster

from test.utils import setup_keyspace
```

Avoid wildcard imports (`from module import *`).

## Documentation
All public functions/classes need docstrings (unless the current directory conventions omit them):
```python
def my_function(arg1: str, arg2: int) -> bool:
    """
    Brief summary of function purpose.

    Args:
        arg1: Description of first argument.
        arg2: Description of second argument.

    Returns:
        Description of return value.
    """
    pass
```

## Testing Best Practices
- Maintain bisectability: all tests must pass in every commit
- Mark currently-failing tests with `@pytest.mark.xfail`, unmark when fixed
- Use descriptive names that convey intent
- Docstrings/comments should explain what the test verifies and why, and if it reproduces a specific issue or how it fits into the larger test suite
