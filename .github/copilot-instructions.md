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

**Build Parallelization:**
- Environment variables `CCACHE_PREFIX=icecc` or `CCACHE_PREFIX=distcc` indicate distributed build setup
- If distributed build is active and verified, use high `-j` count (like `-j150`); otherwise omit `-j`
- Example with distributed build: `ninja -j150 dev-build`

### Legacy Build (dbuild - for old branches)
```bash
# Wrap any command with dbuild for old branches lacking dependencies
./tools/toolchain/dbuild ./configure.py --mode=<mode>
./tools/toolchain/dbuild ninja <mode>-build
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
- To rebuild a specific test binary: `ninja -j150 build/<mode>/test/<suite>/<binary_name>`
- Examples: 
  - `ninja -j150 build/dev/test/boost/combined_tests` (contains group0_voter_calculator_test.cc and others)
  - `ninja -j150 build/dev/test/raft/replication_test` (standalone Raft test)

### Python Integration Tests
```bash
# Only requires Scylla binary (full build NOT needed)
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
- Performance is critical but never at the cost of correctness
- Code must be self-documenting through clear naming
- Avoid comments explaining "what"; use them only for "why"
- Prefer standard library over custom implementations

## Logging
Use structured logging in all languages that support it. Follow these guidelines:
- Use appropriate log levels: DEBUG, INFO, WARN, ERROR
- Include context in log messages (e.g., request IDs)
- Never log sensitive data (credentials, PII)
