# ScyllaDB Development Instructions

## Project Context
High-performance distributed NoSQL database. Core values: performance, correctness, readability.

## Build System

### Modern Build (configure.py + ninja)
```bash
# Configure (run once per mode, or when switching modes)
./configure.py --mode=<mode>  # mode: dev, debug, release, sanitize

# Build everything
ninja <mode>-build  # e.g., ninja dev-build

# Build Scylla binary only (sufficient for Python integration tests)
ninja build/<mode>/scylla

# Build specific test
ninja build/<mode>/test/boost/<test_name>
```

## Running Tests

### C++ Unit Tests
```bash
# Run all tests in a file
./test.py --mode=<mode> test/<suite>/<test_name>.cc

# Run a single test case from a file
./test.py --mode=<mode> test/<suite>/<test_name>.cc::<test_case_name>

# Examples
./test.py --mode=dev test/boost/memtable_test.cc
./test.py --mode=dev test/raft/raft_server_test.cc::test_check_abort_on_client_api
```

**Important:** 
- Use full path with `.cc` extension (e.g., `test/boost/test_name.cc`, not `boost/test_name`)
- To run a single test case, append `::<test_case_name>` to the file path
- If you encounter permission issues with cgroup metric gathering, add `--no-gather-metrics` flag

**Rebuilding Tests:**
- test.py does NOT automatically rebuild when test source files are modified
- Many tests are part of composite binaries (e.g., `combined_tests` in test/boost contains multiple test files)
- To find which binary contains a test, check `configure.py` in the repository root (primary source) or `test/<suite>/CMakeLists.txt`
- To rebuild a specific test binary: `ninja build/<mode>/test/<suite>/<binary_name>`
- Examples: 
  - `ninja build/dev/test/boost/combined_tests` (contains group0_voter_calculator_test.cc and others)
  - `ninja build/dev/test/raft/replication_test` (standalone Raft test)

### Python Integration Tests
```bash
# Only requires Scylla binary (full build usually not needed)
ninja build/<mode>/scylla

# Run all tests in a file
./test.py --mode=<mode> <test_path>

# Run a single test case from a file
./test.py --mode=<mode> <test_path>::<test_function_name>

# Examples
./test.py --mode=dev alternator/
./test.py --mode=dev cluster/test_raft_voters::test_raft_limited_voters_retain_coordinator

# Optional flags
./test.py --mode=dev cluster/test_raft_no_quorum -v  # Verbose output
./test.py --mode=dev cluster/test_raft_no_quorum --repeat 5  # Repeat test 5 times
```

**Important:**
- Use path without `.py` extension (e.g., `cluster/test_raft_no_quorum`, not `cluster/test_raft_no_quorum.py`)
- To run a single test case, append `::<test_function_name>` to the file path
- Add `-v` for verbose output
- Add `--repeat <num>` to repeat a test multiple times
- After modifying C++ source files, only rebuild the Scylla binary for Python tests - building the entire repository is unnecessary

## Code Philosophy
- Performance matters in hot paths (data read/write, inner loops)
- Self-documenting code through clear naming
- Comments explain "why", not "what"
- Prefer standard library over custom implementations
- Strive for simplicity and clarity, add complexity only when clearly justified
- Question requests: don't blindly implement requests - evaluate trade-offs, identify issues, and suggest better alternatives when appropriate
- Consider different approaches, weigh pros and cons, and recommend the best fit for the specific context

## Test Philosophy
- Performance matters. Tests should run as quickly as possible. Sleeps in the code are highly discouraged and should be avoided, to reduce run time and flakiness.
- Stability matters. Tests should be stable. New tests should be executed 100 times at least to ensure they pass 100 out of 100 times. (use --repeat 100 --max-failures 1 when running it)
- Unit tests should ideally test one thing and one thing only.
- Tests for bug fixes should run before the fix - and show the failure and after the fix - and show they now pass.
- Tests for bug fixes should have in their comments which bug fixes (GitHub or JIRA issue) they test.
- Tests in debug are always slower, so if needed, reduce number of iterations, rows, data used, cycles, etc. in debug mode.
- Tests should strive to be repeatable, and not use random input that will make their results unpredictable.
- Tests should consume as little resources as possible. Prefer running tests on a single node if it is sufficient, for example.

