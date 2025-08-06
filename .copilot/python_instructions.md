This document outlines the coding standards and best practices for Python code in this repository, which is primarily located in the `test/` directory. The focus is on writing high-quality, maintainable tests and utility scripts.

# 1. Coding Style and Formatting

* **PEP 8 Compliance:** All Python code must adhere to the PEP 8 style guide.
* **Flake8 Rules:** The code is linted using `flake8`. All suggestions from `flake8` should be addressed. Pay particular attention to:
    * **Line Length:** Lines should ideally not exceed 120 characters to maintain readability.
    * **Indentation:** Use 4 spaces for indentation.
    * **Whitespace:** Be consistent with whitespace around operators, commas, and parentheses.
* **Imports:**
    * Imports should be grouped in the following order: standard library, third-party libraries, and local imports.
    * Use absolute imports where possible. Avoid `from module import *`.
    * All imports should be at the top of the file, after the module-level docstring.

# 2. Documentation and Naming

* **Docstrings:** All public methods, functions, and classes must have a docstring.
    * **Format:** Use triple double quotes (`"""..."""`).
    * **Content:** The docstring should provide a brief summary of the method/function's purpose, its parameters (`Args`), and what it returns (`Returns`).
    * **Example:**
      ```python
      def my_test_method(self, arg1: str, arg2: int) -> bool:
          """
          Description of what the test method does.

          This is a more detailed explanation of the test.

          Args:
              arg1: The first argument, a string.
              arg2: The second argument, an integer.

          Returns:
              True if the condition is met, False otherwise.
          """
          # ... test logic
      ```
* **Naming:**
    * **`snake_case`:** Use `snake_case` for all function names, method names, and variable names.
    * **Test Files:** Test files should be named `test_*.py`.
    * **Test Functions:** Test function names should be descriptive and start with `test_`.
    * **Example:** `test_read_consistency`, `test_schema_changes`.

# 3. Testing Best Practices

* **Test Framework:** The test suite uses `pytest` and its ecosystem. Copilot should generate tests using `pytest` idioms.
* **Test Isolation:** Tests should be self-contained and isolated. Avoid dependencies between tests.
* **Clear Assertions:** Use clear and specific assertion methods (e.g., `assert my_value == expected_value`) to make failures easy to understand.
* **Parameterization:** Use `pytest.mark.parametrize` to test a function with multiple inputs, which reduces code duplication and improves clarity.
* **Mocks and Fixtures:**
    * Use `pytest` fixtures for setup and teardown logic. Fixtures should be defined in `conftest.py` for shared use.
    * Use `unittest.mock` or `pytest-mock` to mock dependencies and external services. This is crucial for isolating the system under test.
* **Error Handling in Tests:**
    * Use `pytest.raises()` to test for expected exceptions.
    * Example:
      ```python
      import pytest

      def test_invalid_input_raises_exception():
          with pytest.raises(ValueError):
              my_function("invalid")
      ```

# 4. ScyllaDB-Specific Testing Patterns

## Test Environment Setup
* **Fixtures:** Use `cql` and `test_keyspace` fixtures provided by the test framework
* **Connection Management:** The framework handles connection lifecycle automatically
* **Keyspace Isolation:** Each test gets its own keyspace for isolation
* **Server Management:** Test framework manages Scylla server instances

## CQL Testing Patterns
* **Schema Setup:** Create tables within test functions using the provided keyspace
* **Data Preparation:** Use CQL INSERT statements or batch operations for test data
* **Query Testing:** Test both successful queries and error conditions
* **Result Validation:** Use `assert` statements to validate query results

## Integration Test Structure
```python
def test_feature_name(cql, test_keyspace):
    """
    Test description explaining what this test validates.
    """
    # Arrange: Set up schema and test data
    cql.execute(f"CREATE TABLE {test_keyspace}.test_table (id int PRIMARY KEY, data text)")
    cql.execute(f"INSERT INTO {test_keyspace}.test_table (id, data) VALUES (1, 'test')")
    
    # Act: Perform the operation being tested
    result = cql.execute(f"SELECT * FROM {test_keyspace}.test_table WHERE id = 1")
    
    # Assert: Validate the results
    assert result.one().data == 'test'
```

## Error Testing Patterns
* **Exception Testing:** Use `pytest.raises()` for expected database errors
* **Invalid Queries:** Test malformed CQL statements
* **Constraint Violations:** Test primary key, foreign key, and other constraint violations
* **Timeout Testing:** Test operations under resource constraints

## Multi-Node Testing (Topology Tests)
* **Cluster Setup:** Use topology test framework for multi-node scenarios
* **Node Operations:** Test node addition, removal, and failure scenarios
* **Consistency Testing:** Validate data consistency across nodes
* **Network Partitions:** Test behavior under network partition scenarios

# 5. Additional Recommendations for Copilot

* **Test Data Factories:** Create helper functions for generating test data consistently
* **Async Testing:** Use `async`/`await` patterns when testing asynchronous operations
* **Cleanup Patterns:** Ensure proper cleanup in teardown methods, though the framework handles most cleanup automatically
* **Performance Considerations:** Be mindful of test execution time, especially in integration tests
* **Database State:** Assume clean database state at test start, avoid dependencies between tests
