This document provides comprehensive testing guidelines specific to ScyllaDB's multi-layered testing architecture.

# 1. Testing Framework Architecture

ScyllaDB uses a sophisticated multi-tier testing system that combines several frameworks:

## Test Types and Frameworks

* **Pure Boost Tests:** Standalone C++ tests using `BOOST_AUTO_TEST_CASE` for utility functions and simple logic
* **Seastar Tests:** Coroutine-aware tests using `SEASTAR_TEST_CASE` for asynchronous functionality
* **CQL Environment Tests:** Full database environment tests using `do_with_cql_env()` for database-level functionality
* **Python Integration Tests:** High-level black-box tests using pytest for end-to-end scenarios
* **Performance Tests:** Benchmarking tests using Seastar's performance testing framework

# 2. Test Selection Guidelines

## When to Use Pure Boost Tests (`BOOST_AUTO_TEST_CASE`)
* Testing standalone utility functions
* Mathematical operations and algorithms
* Simple data structure operations
* String parsing and formatting
* No Seastar dependencies required

**Example:**
```cpp
BOOST_AUTO_TEST_CASE(test_uuid_generation) {
    auto uuid1 = utils::UUID_gen::get_time_UUID();
    auto uuid2 = utils::UUID_gen::get_time_UUID();
    BOOST_CHECK(uuid1 != uuid2);
}
```

## When to Use Seastar Tests (`SEASTAR_TEST_CASE`)
* Testing asynchronous operations and coroutines
* Network I/O operations
* File system operations
* Any functionality requiring Seastar's reactor
* Futures and continuations

**Example:**
```cpp
SEASTAR_TEST_CASE(test_async_operation) {
    return async_function().then([](auto result) {
        BOOST_CHECK_EQUAL(result.value, expected_value);
    });
}
```

## When to Use CQL Environment Tests (`do_with_cql_env`)
* Testing database schema operations
* Query execution and result validation
* Transaction and consistency behavior
* Multi-table operations
* Database-level configuration testing

**Example:**
```cpp
SEASTAR_TEST_CASE(test_table_creation) {
    return do_with_cql_env([] (cql_test_env& e) {
        return e.execute_cql("CREATE TABLE test.t1 (id int PRIMARY KEY)");
    });
}
```

## When to Use Python Integration Tests
* End-to-end user scenarios
* Multi-node cluster testing
* API compatibility testing
* Long-running operations
* Cross-language driver testing

**Example:**
```python
def test_basic_crud_operations(cql, test_keyspace):
    cql.execute(f"CREATE TABLE {test_keyspace}.users (id int PRIMARY KEY, name text)")
    cql.execute(f"INSERT INTO {test_keyspace}.users (id, name) VALUES (1, 'Alice')")
    result = cql.execute(f"SELECT * FROM {test_keyspace}.users WHERE id = 1")
    assert result.one().name == 'Alice'
```

# 3. Test Organization and Naming

## File Naming Conventions
* **C++ Unit Tests:** `test/boost/feature_name_test.cc`
* **Python Integration Tests:** `test/cqlpy/test_feature_name.py`
* **Topology Tests:** `test/cluster/test_feature_name.py`
* **Performance Tests:** `test/perf/perf_feature_name.cc`

## Test Function Naming
* **C++ Tests:** Descriptive test case names reflecting the functionality being tested
* **Python Tests:** `test_` prefix with descriptive names following pytest conventions

## Test Suite Organization
* Group related test cases in the same file
* Use test fixtures for common setup/teardown
* Separate fast tests from slow tests
* Consider test dependencies and isolation

# 4. Test Framework Specifics

## Boost Test Macros
* `BOOST_CHECK`: Non-fatal assertion
* `BOOST_CHECK_EQUAL`: Value equality check
* `BOOST_CHECK_THROW`: Exception testing
* `BOOST_REQUIRE`: Fatal assertion (stops test on failure)

## Seastar Test Patterns
* Always return futures from `SEASTAR_TEST_CASE`
* Use `SEASTAR_THREAD_TEST_CASE` for thread-based async testing
* Prefer coroutines (`co_await`) for readable async code
* Use `do_with` for RAII-style resource management

## CQL Environment Patterns
* Use `cql_test_env` for database setup
* Prefer prepared statements for repeated queries
* Test both positive and negative cases
* Include schema validation in tests

# 5. Performance and Debugging

## Test Performance Considerations
* Keep unit tests fast (< 1 second)
* Use `test/perf/` for performance-critical tests
* Mock external dependencies in unit tests
* Avoid unnecessary database setup in simple tests

## Debugging Failed Tests
* Test logs are in `testlog/${mode}/`
* Use `--verbose` flag for detailed output
* Build with debug symbols for better stack traces
* Use `SEASTAR_THREAD_TEST_CASE` for easier debugging

# 6. Test Data and Fixtures

## Test Data Management
* Use deterministic test data for reproducibility
* Create minimal datasets that exercise the functionality
* Clean up test data in teardown methods
* Use temporary directories for file-based tests

## Common Fixtures
* Database schemas and keyspaces
* Sample data sets
* Mock services and clients
* Resource cleanup handlers

# 7. Test Execution and CI Integration

## Running Tests Locally
* Individual tests: `./test.py --mode dev test/boost/specific_test.cc`
* Test suites: `./test.py --mode dev boost/`
* With filtering: `./test.py --mode dev boost/specific_test.cc::test_case_name`

## CI Considerations
* Tests must be deterministic and not flaky
* Avoid timing-dependent assertions
* Use appropriate timeouts for async operations
* Consider resource constraints in CI environment

# 8. Test-Driven Development Recommendations

## TDD Workflow for ScyllaDB
1. **Write failing test first:** Start with the test that captures the desired behavior
2. **Choose appropriate test type:** Based on the guidelines above
3. **Implement minimal code:** Make the test pass with the simplest implementation
4. **Refactor and improve:** Enhance implementation while keeping tests green
5. **Add edge case tests:** Cover error conditions and boundary cases

## Test Coverage Guidelines
* Aim for comprehensive test coverage of public APIs
* Test error conditions and edge cases
* Include integration tests for complex workflows
* Performance tests for critical paths

# 9. Common Patterns and Anti-Patterns

## Recommended Patterns
* Single responsibility per test case
* Descriptive test names that explain the scenario
* Clear arrange-act-assert structure
* Proper resource cleanup

## Anti-Patterns to Avoid
* Tests that depend on other tests
* Overly complex test setup
* Hard-coded timing assumptions
* Tests that modify global state without cleanup
* Mixing multiple concerns in single test

# 10. Language-Specific Testing Considerations

## C++ Testing Best Practices
* Use RAII for resource management
* Prefer stack allocation over heap when possible
* Use appropriate assertion macros
* Handle exceptions properly in async code

## Python Testing Best Practices
* Use pytest fixtures for setup/teardown
* Leverage pytest marks for test categorization
* Use context managers for resource cleanup
* Follow PEP 8 naming conventions
