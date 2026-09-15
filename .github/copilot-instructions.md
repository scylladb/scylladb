# ScyllaDB Development Instructions

## Project Context
High-performance distributed NoSQL database. Core values: performance, correctness, readability.

## Build System

### Using frozen toolchain (Docker/Podman) — always prefer this
Prefix every build command with `./tools/toolchain/dbuild`:
```bash
./tools/toolchain/dbuild ./configure.py --disable-dpdk
./tools/toolchain/dbuild ninja <mode>-build          # modes: dev, debug, release, sanitize
./tools/toolchain/dbuild ninja build/<mode>/scylla   # binary only (sufficient for Python tests)
```
For native builds, use the same commands without the `dbuild` prefix.

### Building Tests
- `test.py` does NOT rebuild C++ test binaries — rebuild with `ninja` first
- Most boost tests are in composite binaries (e.g., `combined_tests`)
- Check `configure.py` or `test/<suite>/CMakeLists.txt` for the binary name
```bash
ninja build/<mode>/test/<suite>/<binary_name>

# Examples:
ninja build/dev/test/boost/combined_tests    # composite (many boost tests)
ninja build/dev/test/raft/replication_test   # standalone
```

## Running Tests

### C++ Unit Tests
```bash
# Run all tests in a file
./test.py --mode=<mode> test/<suite>/<test_name>.cc

# Run a single test case
./test.py --mode=<mode> test/<suite>/<test_name>.cc::<test_case_name>

# Examples
./test.py --mode=dev test/boost/memtable_test.cc
./test.py --mode=dev test/raft/raft_server_test.cc::test_check_abort_on_client_api
```

**Important:** Use full path with `.cc` extension. Add `--no-gather-metrics` if cgroup errors occur.

### Python Integration Tests
```bash
# Only requires Scylla binary (not full build)
ninja build/<mode>/scylla

./test.py --mode=<mode> test/<suite>/<test_name>.py
./test.py --mode=<mode> test/<suite>/<test_name>.py::<test_function_name>

# Examples (directory / file / single-test granularity)
./test.py --mode=dev test/alternator/
./test.py --mode=dev test/cqlpy/test_json.py
./test.py --mode=dev test/cluster/test_raft_voters.py::test_raft_limited_voters_retain_coordinator

# Optional flags: -v (verbose), --repeat <n>, --max-failures <n>
./test.py --mode=dev test/cluster/test_raft_no_quorum.py -v --repeat 5
```

**Important:** Use full path with `.py` extension. After C++ changes, rebuild the scylla binary.

### Test Suites
| Suite | Type | Purpose |
|-------|------|---------|
| `test/boost/` | C++ | Core unit tests (Boost.Test) |
| `test/raft/` | C++ | Raft consensus tests |
| `test/unit/` | C++ | Stress, LSA, row cache tests |
| `test/alternator/` | Python | DynamoDB-compatible API |
| `test/cqlpy/` | Python | CQL functional tests |
| `test/cluster/` | Python | Topology tests (multi-node) |
| `test/rest_api/` | Python | REST API tests |
| `test/nodetool/` | Python | Nodetool tests |
| `test/perf/` | C++ | Performance benchmarks |

## Code Philosophy
- Performance matters in hot paths (data read/write, inner loops)
- Self-documenting code through clear naming
- Comments explain "why", not "what"
- Prefer standard library over custom implementations
- Strive for simplicity and clarity, add complexity only when clearly justified
- Question requests: don't blindly implement - evaluate trade-offs, suggest better alternatives
- Consider different approaches and recommend the best fit for context

## Test Philosophy
- Performance matters. Tests should run as quickly as possible. Sleeps are highly discouraged.
- Stability matters. Tests should be stable. New tests should pass 100/100 times (`--repeat 100 --max-failures 1`).
- Unit tests should test one thing only.
- Bug fix tests: run before fix (shows failure) and after fix (shows pass).
- Bug fix tests should reference the issue (GitHub or JIRA) in comments.
- Debug mode is slower — reduce iterations, rows, data, cycles as needed.
- Tests should be repeatable, avoiding unpredictable random input.
- Tests should consume minimal resources. Prefer single-node when sufficient.

## Development Notes
- `.clang-format` at repo root (160-char, 4-space indent); CI lint: `clang-tidy`, `clang-include-cleaner`, `codespell`; header check: `ninja dev-headers`
- Commit subject: `"module: short description"`; `Fixes: #nnnn` for bug fixes (see `docs/dev/backport.md`); bisectability per commit
- Branch naming from `HACKING.md`: `initials/feature/v1` (e.g., `ts/cql_create_table_error/v1`)
- Full conventions at `docs/dev/review-checklist.md`; debugging at `docs/dev/debugging.md`
- Run Scylla: `--smp 1 --memory 1G --overprovisioned --developer-mode=yes`; debug info: `_g` suffix on test binaries
- GDB: source `gdbinit` (repo root) in `~/.gdbinit`; extension: `scylla-gdb.py`
- Related: `.github/instructions/cpp.instructions.md`, `.github/instructions/python.instructions.md`

## New Files
- SPDX header: `LicenseRef-ScyllaDB-Source-Available-1.1`
- Use current year for new files; keep existing years as-is
