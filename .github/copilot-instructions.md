# ScyllaDB Development Instructions

## Project Context
High-performance distributed NoSQL database. Core values: performance, correctness, readability.

## Build System

### Using native OS environment
```bash
# Configure (run once)
./configure.py

# Build everything
ninja <mode>-build  # modes: dev, debug, release, sanitize
                    # dev is recommended for development (fastest compilation)

# Build Scylla binary only (sufficient for Python integration tests)
ninja build/<mode>/scylla

# Build specific test
ninja build/<mode>/test/boost/<test_name>
```

### Using frozen toolchain (Docker)
Prefix any build command with `./tools/toolchain/dbuild`.

- `dbuild` picks its toolchain image from the `image` file next to it, so use the `dbuild` of the
  tree you are building, not a sibling checkout's: another branch has another toolchain.
- Run everything built in the container under `dbuild` too: the `scylla` binary, `test.py`
  (which launches the test binaries, e.g. `./tools/toolchain/dbuild ./test.py --mode=dev
  test/boost/memtable_test.cc`), and the perf microbenchmarks such as
  `./tools/toolchain/dbuild ./build/release/scylla perf-simple-query` or the binaries under
  `build/<mode>/test/perf/`. The host usually lacks the toolchain's library versions, and a
  benchmark that happens to run on the host measures the wrong libraries.
- When switching a checkout between `master` and a release branch, reset the build tree first:
  see "Switching between branches" in `docs/dev/building.md`.

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
- Use full path with `.cc` extension (e.g., `test/boost/memtable_test.cc`)
- To run a single test case, append `::<test_case_name>` to the file path
- If you encounter permission issues with cgroup metrics, add `--no-gather-metrics` to the `./test.py` command

**Rebuilding Tests:**
- test.py does NOT automatically rebuild when test source files are modified
- Many tests are part of composite binaries (e.g., `combined_tests` in test/boost contains multiple test files)
- To find which binary contains a test, check `configure.py` in the repository root (primary source) or `test/<suite>/CMakeLists.txt`
- To rebuild a specific test binary: `ninja build/<mode>/test/<suite>/<binary_name>`
- Build the stripped binary (e.g. `combined_tests`); that is what test.py runs. The `_g` variant
  (e.g. `combined_tests_g`) is unstripped and only for debugging with gdb or `seastar-addr2line`
- Unless told otherwise, build and test in `dev` mode. When fixing a CI failure, note the failing
  build mode (often `debug`) and verify the fix in that mode as well as in `dev`
- Examples: 
  - `ninja build/dev/test/boost/combined_tests` (contains group0_voter_calculator_test.cc and others)
  - `ninja build/dev/test/raft/replication_test` (standalone Raft test)

### Python Integration Tests
```bash
# Only requires Scylla binary (full build usually not needed)
ninja build/<mode>/scylla

# Run all tests in a file
./test.py --mode=<mode> test/<suite>/<test_name>.py

# Run a single test case from a file
./test.py --mode=<mode> test/<suite>/<test_name>.py::<test_function_name>

# Examples
./test.py --mode=dev test/alternator/
./test.py --mode=dev test/cqlpy/test_json.py
./test.py --mode=dev test/cluster/test_raft_voters.py::test_raft_limited_voters_retain_coordinator

# Optional flags
./test.py --mode=dev test/cluster/test_raft_no_quorum.py -v --repeat 5
```

**Important:**
- Use full path with `.py` extension
- To run a single test case, append `::<test_function_name>` to the file path
- Add `-v` for verbose output
- Add `--repeat <num>` to repeat a test multiple times
- After modifying C++ source files, only rebuild the Scylla binary for Python tests

## Code Philosophy
- Performance matters in hot paths (data read/write, inner loops)
- Self-documenting code through clear naming
- Comments explain "why", not "what"
- Prefer standard library over custom implementations
- Strive for simplicity and clarity, add complexity only when clearly justified
- Question requests: don't blindly implement requests - evaluate trade-offs, identify issues, and suggest better alternatives when appropriate
- Consider different approaches, weigh pros and cons, and recommend the best fit for the specific context

## Making Changes
- Make only the functional changes the task needs, and motivate each one in the patch
  description.
- Keep the existing logic intact unless the change requires otherwise. Do not reorder statements,
  restructure code or rename things that work, as this hides the real change and risks regressions.
- Do not reformat or restyle code you are not otherwise changing. Cosmetic changes are made only
  when asked for, and then as separate patches (see "Patch organization" in
  `docs/dev/review-checklist.md`).
- Adding or updating comments that explain the change is welcome.

## Test Philosophy
- Performance matters. Tests should run as quickly as possible. Sleeps in the code are highly discouraged and should be avoided, to reduce run time and flakiness.
- Stability matters. Tests should be stable. New tests should be executed 100 times at least to ensure they pass 100 out of 100 times. (use --repeat 100 --max-failures 1 when running it)
- Unit tests should ideally test one thing only.
- Tests for bug fixes should run before the fix - and show the failure and after the fix - and show they now pass.
- Tests for bug fixes should have in their comments which bug fixes (GitHub or JIRA issue) they test. This is for tests that are added to prevent a regression from a reported bug; fixes for existing flaky tests, or regression tests added as part of a new feature are exempt.
- Tests in debug are always slower, so if needed, reduce number of iterations, rows, data used, cycles, etc. in debug mode.
- Tests should strive to be repeatable, and not use random input that will make their results unpredictable.
- Tests should consume as little resources as possible. Prefer running tests on a single node if it is sufficient, for example.

## Git and GitHub Workflow
- Never push a branch, open a pull request or force-push without being asked to.
- Push to the contributor's fork, never to `scylladb/scylladb` directly. Pull requests
  target `master`; backport pull requests target the release branch (`branch-X.Y`).
- Backport pull requests opened by the bot have their branch on the `scylladbbot` fork
  (`backport/<parent PR>/to-<version>`). To fix such a pull request, for example to resolve
  conflicts, update that branch in place rather than opening a new pull request.
- After a force-push, comment on the pull request describing what changed since the previous
  push and what verification was run locally.
- When rebasing onto a new baseline, run `git submodule sync --recursive` and
  `git submodule update --init --recursive` afterwards so the submodules match the new baseline.

## Pull Requests and Issues
- Keep pull request descriptions, cover letters and issue reports short and focused. Verbose
  text makes human review harder and contributes to review fatigue.
- In a pull request description, do not include a commit list; the pull request already shows
  one. Do not list the tests you ran locally either; CI runs them. Mention only what CI does
  not run, such as manual perf runs (`perf_*`), and their results.
- In an issue report, do reference the commits that matter, such as the one that introduced
  the problem or a related fix, by URL.
- Do not add sections the reader does not need.
- Format for the reader, in comments as well as descriptions: break the text into short
  paragraphs by topic and use a list where the items are parallel. Several sentences run
  together on one line are as hard to read as a wall of text.

## External Communication
- When an AI agent posts a comment on GitHub or Jira, it says so and names the model, and
  states on whose behalf it comments, e.g. `(<model name>, on behalf of @<user>)`.
- In Jira issues and comments, reference commits and other issues by their full URLs, e.g.
  `https://github.com/scylladb/scylladb/commit/<sha>` and
  `https://scylladb.atlassian.net/browse/SCYLLADB-1234`. Jira renders them as inline smart
  cards showing the commit title or the issue title and status, so do not spell those out by
  hand. A bare key such as `SCYLLADB-1234` is not linked. In markdown a bare URL becomes a
  smart card automatically; in HTML an `<a>` needs `data-card-appearance="inline"`, otherwise
  it stays a plain hyperlink.

## New Files
- Include `LicenseRef-ScyllaDB-Source-Available-1.1` in the SPDX header
- Use the current year for new files; for existing code keep the year as is
