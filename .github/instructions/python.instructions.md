---
applyTo: "**/*.py"
---

# Python Guidelines

## Style
- Follow PEP 8
- Use type hints for function signatures
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

Never use `from module import *`

## Test Structure
```python
def test_feature_name(cql, test_keyspace):
    """Brief description of what is tested."""
    # Arrange: setup schema and data
    cql.execute(f"CREATE TABLE {test_keyspace}.tbl (id int PRIMARY KEY, data text)")
    cql.execute(f"INSERT INTO {test_keyspace}.tbl (id, data) VALUES (1, 'test')")
    
    # Act: perform operation
    result = cql.execute(f"SELECT * FROM {test_keyspace}.tbl WHERE id = 1")
    
    # Assert: verify results
    assert result.one().data == 'test'
```

## Documentation
All public functions/classes need docstrings:
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

## Naming
- `snake_case` for functions, methods, variables
- Test files: `test_*.py`
- Test functions: `test_descriptive_name`

## Forbidden
- `print()` (use logging)
- Bare `except:` (catch specific exceptions)
- Mutable default arguments
