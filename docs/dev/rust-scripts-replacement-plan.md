# Replace Python Scripts with Rust Binaries

## Motivation: Why Replace Python with Rust

### Problem

ScyllaDB production nodes currently require a Python 3 runtime solely to run ~25 admin/setup scripts. This creates several concrete problems:

1. **Path to dropping vendored Python entirely** — We currently ship a full Python interpreter on every production node. Once these scripts, `cqlsh` (#29523), and Seastar's Python scripts are all migrated, we can stop vendoring Python altogether.

2. **Supply-chain surface area** — Six PyPI packages (`pyudev`, `psutil`, `distro`, `PyYAML`, `traceback_with_variables`, `setuptools`) must be vendored or installed on every production node. Each is an attack vector and a version-compatibility liability.

3. **Startup-path fragility** — `scylla_prepare` runs as `ExecStartPre` in the systemd unit. A broken Python environment (missing module, wrong version, virtualenv corruption) prevents ScyllaDB from starting at all, with an unhelpful traceback as the only clue.

### Why Rust Specifically

- **Zero runtime dependencies** — A statically-linked binary needs nothing on the target node. No interpreter, no virtualenv, no pip.
- **Compile-time correctness** — The category of bugs exemplified by `scylla_ntp_setup`'s undefined `confpath` variable (two code paths) cannot exist in Rust. Static typing catches these at compile time, not in production.
- **Cargo ecosystem** — `serde_yaml`, `nix`, `clap` cover 100% of what the Python scripts need from `PyYAML`, `psutil`/`os`, and `argparse`, with the same or better ergonomics.
- **Already in the build** — ScyllaDB already builds Rust code (`cqlsh-rs`, Seastar dependencies). The toolchain is present; adding a Cargo workspace is incremental.
- **Debuggability as a first-class feature** — Every Rust binary gets `--verbose`, `--dry-run`, `--diagnose`, and crash context dumps for free via a shared lib crate. This is the "major improvement" over a same-thing-in-different-language port.

### What This Is NOT

- Not a rewrite of `perftune.py` (out of scope — separate discussion)
- Not a rewrite of `cqlsh` (tracked in #29523)
- Not adding new features — CLI flags, exit codes, and output format remain identical
- Not removing Python from the build system — only from production runtime, and only after all other components (`cqlsh`, Seastar scripts) are also migrated off Python

---

## TL;DR

> **Quick Summary**: Replace all ~25 Python scripts in `dist/common/scripts/` with Rust binaries via a Cargo workspace, eliminating the Python runtime dependency from production nodes while delivering a major improvement in debuggability, error UX, testability, and maintainability.
> 
> **Deliverables**:
> - Cargo workspace at `dist/common/scripts-rs/` with shared lib crate + ~22 bin crates
> - Every binary has: `--verbose`, `--dry-run`, `--diagnose` flags + crash context dumps
> - Unit tests for lib crate, integration tests per binary
> - Identical CLI interface (flags, exit codes, output format) to Python originals
> - Python scripts kept as fallback during incremental rollout
> 
> **Estimated Effort**: XL
> **Parallel Execution**: YES - 5 waves
> **Critical Path**: Task 1 (workspace) → Task 2 (lib core) → Task 3 (debug infra) → Wave 2 leaf scripts → Wave 3 mid scripts → Wave 4 complex → Wave 5 orchestrator

---

## Context

### Original Request
Replace all Python scripts in `dist/common/scripts/` with Rust binaries to drop the Python interpreter dependency from ScyllaDB production nodes. Executable names must remain identical for compatibility.

### Interview Summary
**Key Discussions**:
- Migration must be a "major improvement" — not just same-thing-in-Rust
- Top priorities: zero runtime deps, better error UX, testability from day one, debuggability leap, maintainability
- ALL debug features required: verbose mode, crash context files, debug symbols, dry-run, diagnostics
- Incremental rollout with Python fallback
- Standalone Cargo build first, ninja integration later

**Research Findings**:
- 25+ Python scripts, all sharing `scylla_util.py` (526 lines) as common library
- `scylla_setup` (568 lines) is an orchestrator calling 13+ other scripts — must be migrated LAST
- `scylla_blocktune.py` is both a module (imported by `scylla_io_setup`) AND a CLI binary (`scylla-blocktune`) — needs shared lib code
- `scylla_sysconfdir.py` is generated at install time by `install.sh` with configured SYSCONFDIR
- `scylla-housekeeping` and `scylla_config_get.py` do NOT import `scylla_util` — fully standalone
- External Python deps on prod nodes: pyudev, psutil, distro, PyYAML, traceback_with_variables, pkg_resources
- Scripts invoked by systemd: `scylla_prepare` (ExecStartPre), `scylla_stop` (ExecStopPost), `scylla_fstrim` (ExecStart), `scylla-housekeeping` (2 timer services)
- `install.sh` copies scripts to `$rprefix/scripts` and some to sbin; `scylla_sysconfdir.py` also sourced as bash

### Metis Review
**Identified Gaps** (addressed):
- `scylla_setup` orchestration constraint → Wave 5, must be last
- `scylla_blocktune` module/binary duality → Shared code in lib crate, two bin crates
- `scylla_sysconfdir.py` is build-time generated → Rust reads `scylla_sysconfdir.conf` at runtime (generated by install.sh)
- `scylla-housekeeping` is standalone (no scylla_util) → Moved to Wave 2
- Exit code / stdout / stderr format contracts → Explicit guardrails
- Nonroot mode diverges all path resolution → Must test both paths
- Interactive mode in `scylla_setup` → Must preserve `input()` and ANSI color behavior

---

## Work Objectives

### Core Objective
Replace every Python script in `dist/common/scripts/` with a behaviorally-identical Rust binary, shipped as part of a Cargo workspace at `dist/common/scripts-rs/`, with comprehensive debug capabilities and test coverage.

### Concrete Deliverables
- `dist/common/scripts-rs/Cargo.toml` — workspace root
- `dist/common/scripts-rs/scylla-scripts-lib/` — shared library crate
- `dist/common/scripts-rs/scylla-{name}/` — one bin crate per script (~22 total)
- Unit tests in lib crate, integration tests per binary
- Debug symbols build configuration (separate profile)
- Documentation: migration guide for packaging team

### Definition of Done
- [ ] Every Rust binary produces identical `--help` output to its Python predecessor **except** for the added debug flags (`--verbose`, `--dry-run`, `--diagnose`) which are allowed additions
- [ ] Every Rust binary exits with identical exit codes for identical inputs
- [ ] `systemctl start/stop scylla-server` works with Rust `scylla_prepare` and `scylla_stop`
- [ ] `cargo test` passes all unit + integration tests
- [ ] Each binary supports `--verbose`, `--dry-run`, `--diagnose` (where applicable)
- [ ] On panic/error, crash context dumped to `/var/tmp/scylla/{binary}-{pid}-debug.log`
- [ ] `ldd` shows minimal dynamic deps (libc only or static)
- [ ] Tested on debian-variant and redhat-variant

### Must Have
- Identical CLI flags and argument semantics (clap matching argparse exactly) — **debug flags (`--verbose`, `--dry-run`, `--diagnose`) are allowed additions that won't exist in Python originals**
- Identical exit codes for all code paths
- Identical file read/write paths (`/etc/scylla.d/`, `/etc/sysconfig/scylla-server`, etc.)
- All distro detection branches: debian, redhat, gentoo, suse, arch, amzn2
- Nonroot mode support (alternative path resolution)
- Offline mode support (no package installs)
- Container mode detection
- `scylla_sysconfdir` equivalent (runtime config file `scylla_sysconfdir.conf`, overridable via `SCYLLA_SCRIPTS_TEST_SYSCONFDIR` env var for tests)

### Must NOT Have (Guardrails)
- **No feature additions** — this is a behavioral rewrite, not enhancement. Enhancements come later.
- **No CLI flag changes** — same names, same defaults, same mutual exclusions
- **No stdout/stderr format changes** — operators may grep for specific strings
- **No script consolidation** — 1:1 binary replacement, no merging scripts
- **No systemd unit changes** — binaries must work with existing service files
- **No rewriting node_health_check** — stays as bash
- **No C++ integration** — scripts remain standalone binaries
- **Do not migrate `scylla_setup` before ALL sub-scripts it calls are migrated**

---

## Verification Strategy (MANDATORY)

> **ZERO HUMAN INTERVENTION** - ALL verification is agent-executed. No exceptions.

### Test Decision
- **Infrastructure exists**: NO (new Cargo workspace)
- **Automated tests**: YES (tests-after, not TDD — matching behavioral contracts)
- **Framework**: `cargo test` (built-in Rust test framework)

### QA Policy
Every task MUST include agent-executed QA scenarios.
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

- **CLI binaries**: Use Bash — run binary with specific args, assert exit code + stdout/stderr
- **`--help` parity checks**: All `--help` diff scenarios across tasks MUST filter out Rust-only debug flags before comparing: `diff <(python3 ... --help 2>&1) <(./target/release/... --help 2>&1 | grep -v -E '^\s*--(verbose|dry-run|diagnose)')`. When a task says "Zero diff", this filtered comparison is implied. **Exception**: Scripts without argparse (`scylla_kernel_check`, `scylla_logrotate`, `scylla_fstrim_setup`, `scylla_stop`, `scylla_prepare`, `scylla_selinux_setup`, `scylla_fstrim`) have no `--help`; their parity is verified via exit-code and output-format checks instead.
- **Lib crate**: Use `cargo test` — unit tests for each module
- **Integration**: Use Bash — compare Rust binary output vs Python script output on same inputs
- **Systemd**: Use interactive_bash (tmux) — start/stop scylla-server service, verify journal logs

### Test Injection Mechanism (MANDATORY — all binaries must support this)

> **Problem**: QA scenarios need to test binaries against fake/temp directories without root.
> **Solution**: All path-resolving functions in the shared lib crate MUST support environment variable overrides for testability.

**Standard env var overrides** (implemented in Task 2's path resolution module):
- `SCYLLA_SCRIPTS_TEST_HOME` → overrides `scylladir` (base for `etcdir`, `datadir`, etc.)
- `SCYLLA_SCRIPTS_TEST_ETC` → overrides `etcdir()` directly (e.g., `/etc/scylla`)
- `SCYLLA_SCRIPTS_TEST_SYSCONFIG` → overrides sysconfig file path (e.g., `/etc/sysconfig/scylla-server`)
- `SCYLLA_SCRIPTS_TEST_SYSCONFDIR` → overrides `sysconfdir()` (the value normally read from `scylla_sysconfdir.conf`)

> Note: `SCYLLA_CONF` already has meaning in the Python scripts (config directory). These test overrides use a distinct `SCYLLA_SCRIPTS_TEST_*` prefix to avoid collision.

**Usage in QA scenarios**: Instead of vague "create fake etcdir", QA steps should:
```bash
export SCYLLA_SCRIPTS_TEST_ETC=$(mktemp -d)
# ... populate with test files ...
./target/release/scylla_memory_setup --lock-memory 1
# Assert: $SCYLLA_SCRIPTS_TEST_ETC/scylla.d/memory.conf contains expected content
```

**Usage in unit tests**: Shared lib functions accept an optional config/path struct that can be overridden in tests:
```rust
// Production: paths::etcdir() reads env or returns default
// Test: paths::etcdir_with_override("/tmp/test-etc")
```

**This mechanism is NOT a new feature** — it mirrors Python's existing `scylla_sysconfdir.py` pattern and standard practice for testable system tools.

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Foundation — sequential, must complete first):
├── Task 1: Cargo workspace scaffolding [quick]
├── Task 2: Lib crate core — path resolution, distro detection, command execution [deep]
├── Task 3: Lib crate debug infra — verbose, crash dumps, dry-run, diagnostics [deep]
├── Task 4: Lib crate config — sysconfig_parser, YAML parsing, sysconfdir [deep]
├── Task 5: Lib crate system — systemd_unit wrapper, pkg_install, disk utils [deep]

Wave 2 (Leaf scripts — MAX PARALLEL, after Wave 1):
├── Task 6: scylla_logrotate (17 lines) [quick]
├── Task 7: scylla_fstrim_setup (19 lines) [quick]
├── Task 8: scylla_stop (25 lines, systemd critical!) [quick]
├── Task 9: scylla_dev_mode_setup (30 lines) [quick]
├── Task 10: scylla_rsyslog_setup (30 lines) [quick]
├── Task 11: scylla_selinux_setup (29 lines) [quick]
├── Task 12: scylla_config_get.py (45 lines, standalone) [quick]
└── Task 13: scylla-housekeeping (213 lines, standalone HTTP) [unspecified-high]

Wave 3 (Mid-complexity — MAX PARALLEL, after Wave 1):
├── Task 14: scylla_kernel_check (37 lines) [quick]
├── Task 15: scylla_memory_setup (42 lines) [quick]
├── Task 16: scylla_cpuset_setup (46 lines) [quick]
├── Task 17: scylla_nofile_setup (43 lines) [quick]
├── Task 18: scylla_fstrim (46 lines) [quick]
├── Task 19: scylla_cpuscaling_setup (98 lines) [unspecified-high]
├── Task 20: scylla_ntp_setup (118 lines) [unspecified-high]
├── Task 21: scylla_sysconfig_setup (130 lines) [unspecified-high]
└── Task 22: scylla_swap_setup (147 lines) [unspecified-high]

Wave 4 (Complex — after Waves 2+3):
├── Task 23: scylla_blocktune lib module + scylla-blocktune binary (89+34 lines) [deep]
├── Task 24: scylla_coredump_setup (199 lines) [unspecified-high]
├── Task 25: scylla_io_setup (228 lines, depends on Task 23 blocktune) [deep]
├── Task 26: scylla_raid_setup (425 lines, pyudev/psutil replacement) [deep]
└── Task 27: scylla_prepare (190 lines, systemd critical, generates perftune.yaml) [deep]

Wave 5 (Orchestrator — after ALL above):
└── Task 28: scylla_setup (568 lines, calls 13+ other scripts) [deep]

Wave 6 (Build + Packaging integration):
├── Task 29: install.sh integration — install Rust binaries alongside/replacing Python [unspecified-high]
└── Task 30: Documentation — migration guide for packaging team [writing]

Wave FINAL (After ALL tasks — 4 parallel reviews, then user okay):
├── Task F1: Plan compliance audit (oracle)
├── Task F2: Code quality review (unspecified-high)
├── Task F3: Real manual QA (unspecified-high)
└── Task F4: Scope fidelity check (deep)
-> Present results -> Get explicit user okay

Critical Path: T1 → T2-T5 → T8 (scylla_stop, proves systemd) → Wave 3-4 → T28 (scylla_setup) → T29 → FINAL
Parallel Speedup: ~60% faster than sequential
Max Concurrent: 9 (Wave 2: 8 + Wave 3: 9 can overlap after Wave 1)
```

### Dependency Matrix

| Task | Depends On | Blocks |
|------|-----------|--------|
| 1 | — | 2-5 |
| 2 | 1 | 6-28 |
| 3 | 1 | 6-28 |
| 4 | 1 | 6-28 |
| 5 | 1 | 6-28 |
| 6-12 | 2,3,4,5 | 28 |
| 13 | 1 (standalone, no scylla_util) | 28 |
| 14-15,17-22 | 2,3,4,5 | 28 |
| 16 | 2,3,4,5 | 21,25,27,28 |
| 23 | 2,3,4,5 | 25 |
| 24 | 2,3,4,5 | 28 |
| 25 | 23 | 28 |
| 26 | 2,3,4,5 | 28 |
| 27 | 2,3,4,5 | 28 |
| 28 | 6-27 (ALL) | 29 |
| 29 | 28 | F1-F4 |
| 30 | 28 | — |

### Agent Dispatch Summary

- **Wave 1**: **5 tasks** — T1 → `quick`, T2-T5 → `deep`
- **Wave 2**: **8 tasks** — T6-T12 → `quick`, T13 → `unspecified-high`
- **Wave 3**: **9 tasks** — T14-T18 → `quick`, T19-T22 → `unspecified-high`
- **Wave 4**: **5 tasks** — T23,T25-T27 → `deep`, T24 → `unspecified-high`
- **Wave 5**: **1 task** — T28 → `deep`
- **Wave 6**: **2 tasks** — T29 → `unspecified-high`, T30 → `writing`
- **FINAL**: **4 tasks** — F1 → `oracle`, F2 → `unspecified-high`, F3 → `unspecified-high`, F4 → `deep`

---

## TODOs

- [ ] 1. Scaffold Cargo Workspace

  **What to do**:
  - Create `dist/common/scripts-rs/Cargo.toml` as workspace root
  - Create `dist/common/scripts-rs/scylla-scripts-lib/` as shared lib crate with empty module stubs: `paths.rs`, `distro.rs`, `command.rs`, `config.rs`, `systemd.rs`, `disk.rs`, `pkg.rs`, `debug.rs`
  - Create `dist/common/scripts-rs/scylla-scripts-lib/src/lib.rs` re-exporting all modules
  - Add `Cargo.toml` with key deps: `clap`, `serde`, `serde_yaml`, `nix`, `log`, `env_logger`
  - Add workspace-level `[profile.release]` with `strip = "debuginfo"` and `[profile.release-debug]` with `debug = true` for debuginfo packages
  - Add `.cargo/config.toml` for target-specific link flags if needed for static linking
  - Create a trivial test binary crate `dist/common/scripts-rs/scylla-hello/` to verify workspace builds

  **Must NOT do**:
  - Do not add all 22 bin crates yet — just the lib + one test binary
  - Do not implement any logic — just module stubs

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 1 (sequential start)
  - **Blocks**: Tasks 2, 3, 4, 5
  - **Blocked By**: None

  **References**:
  - `dist/common/scripts/scylla_util.py` — module structure to mirror: path resolution, distro detection, config parsing, systemd, pkg management, disk utilities
  - `dist/common/scripts/scylla_sysconfdir.py` — build-time constant, needs `build.rs` equivalent
  - `install.sh:502` — `cp -pr dist/common/scripts/* "$rprefix"/scripts` — how scripts are installed
  - `install.sh:578-612` — how `scylla_sysconfdir.py` is generated at install time

  **Acceptance Criteria**:
  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Workspace builds successfully
    Tool: Bash
    Steps:
      1. cd dist/common/scripts-rs && cargo build
      2. Assert exit code 0
      3. ls target/debug/scylla-hello
      4. Assert binary exists
    Expected Result: Clean build, test binary produced
    Evidence: .sisyphus/evidence/task-1-workspace-build.txt

  Scenario: Cargo test passes on empty lib
    Tool: Bash
    Steps:
      1. cd dist/common/scripts-rs && cargo test
      2. Assert exit code 0
    Expected Result: 0 tests run, 0 failures
    Evidence: .sisyphus/evidence/task-1-cargo-test.txt
  ```

  **Commit**: YES
  - Message: `feat(scripts-rs): scaffold Cargo workspace with lib crate stubs`
  - Files: `dist/common/scripts-rs/**`

- [ ] 2. Lib Crate Core — Path Resolution, Distro Detection, Command Execution

  **What to do**:
  - Implement `paths.rs`: `scriptsdir()`, `scylladir()`, `bindir()`, `etcdir()`, `datadir()`, `scyllabindir()`, `sysconfdir()` — all with `_p()` Path variants. Must handle nonroot mode (`SCYLLA-NONROOT-FILE`), offline mode (`SCYLLA-OFFLINE-FILE`), container mode (`SCYLLA-CONTAINER-FILE`). **Must support env var overrides for testability**: `SCYLLA_SCRIPTS_TEST_HOME` overrides base dir, `SCYLLA_SCRIPTS_TEST_ETC` overrides etcdir, `SCYLLA_SCRIPTS_TEST_SYSCONFDIR` overrides sysconfdir, `SCYLLA_SCRIPTS_TEST_SYSCONFIG` overrides sysconfig path. See Verification Strategy → Test Injection Mechanism.
  - Implement `distro.rs`: `is_debian_variant()`, `is_redhat_variant()`, `is_gentoo()`, `is_suse_variant()`, `is_arch()`, `is_amzn2()`, `get_id_like()`, `pkg_distro()`. Read `/etc/os-release` directly (no Python `distro` library dependency).
  - Implement `command.rs`: `out()` function equivalent — run shell command, capture stdout, check exit code, print stderr on error. Must match Python's `out()` exactly: `shell=True`, `timeout`, `encoding`, `ignore_error`, `user`, `group` params.
  - Implement `hex2list()` in a `cpu.rs` module.
  - Unit tests for all functions, especially nonroot vs root path divergence and all distro detection branches.

  **Must NOT do**:
  - Do not implement config parsing (Task 4)
  - Do not implement systemd wrapper (Task 5)
  - Do not implement debug infrastructure (Task 3)

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Tasks 3, 4, 5 after Task 1)
  - **Parallel Group**: Wave 1
  - **Blocks**: Tasks 6-28
  - **Blocked By**: Task 1

  **References**:
  - `dist/common/scripts/scylla_util.py:53-116` — path resolution functions, nonroot detection
  - `dist/common/scripts/scylla_util.py:117-143` — distro detection functions
  - `dist/common/scripts/scylla_util.py:38-50` — `out()` command execution function
  - `dist/common/scripts/scylla_util.py:165-183` — `hex2list()` CPU mask conversion
  - `/etc/os-release` format spec — key fields: `ID`, `ID_LIKE`, `VERSION_ID`

  **Acceptance Criteria**:
  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Path resolution in root mode
    Tool: Bash (cargo test)
    Steps:
      1. cargo test paths:: -- --nocapture
      2. Assert tests for bindir=/usr/bin, etcdir=/etc, datadir=/var/lib/scylla when no NONROOT file
    Expected Result: All root-mode paths match Python behavior
    Evidence: .sisyphus/evidence/task-2-paths-root.txt

  Scenario: Path resolution in nonroot mode
    Tool: Bash (cargo test)
    Steps:
      1. cargo test paths::nonroot -- --nocapture
      2. Assert paths are relative to scylladir when NONROOT file exists
    Expected Result: All nonroot paths match Python behavior
    Evidence: .sisyphus/evidence/task-2-paths-nonroot.txt

  Scenario: Distro detection matches /etc/os-release parsing
    Tool: Bash (cargo test)
    Steps:
      1. cargo test distro:: -- --nocapture
      2. Tests mock /etc/os-release with debian, redhat, suse, gentoo, arch, amzn2 variants
    Expected Result: All distro functions return correct booleans
    Evidence: .sisyphus/evidence/task-2-distro.txt
  ```

  **Commit**: YES (groups with T3, T4, T5)
  - Message: `feat(scripts-rs): implement lib crate core — paths, distro, command execution`
  - Files: `dist/common/scripts-rs/scylla-scripts-lib/src/paths.rs`, `distro.rs`, `command.rs`, `cpu.rs`

- [ ] 3. Lib Crate Debug Infrastructure — Verbose, Crash Dumps, Dry-Run, Diagnostics

  **What to do**:
  - Implement `debug.rs` module with:
    - **Verbose mode**: `init_logging(verbose: bool)` — when `--verbose` or `SCYLLA_SCRIPTS_DEBUG=1`, log every decision point, command run, file read/write to stderr
    - **Crash context**: Custom panic hook that dumps: backtrace, all thread-local state, command-line args, env vars, relevant config files content to `/var/tmp/scylla/{binary}-{pid}-debug.log`. Must be equivalent to or better than Python's `scylla_excepthook` + `traceback_with_variables`.
    - **Dry-run**: `DryRun` trait/context — when `--dry-run`, `command::out()` prints what it WOULD run but doesn't execute. File writes print what they WOULD write. Package installs print what they WOULD install.
    - **Diagnostics**: `diagnose()` function — dumps: detected distro, all resolved paths (root/nonroot), scylla.yaml parsed dirs, available NICs, disk devices, systemd unit status, product file content.
  - Add `clap` argument group (`DebugArgs`) that every binary can include: `--verbose`, `--dry-run`, `--diagnose`
  - Unit tests for crash context dump (trigger panic in test, verify log file created)

  **Must NOT do**:
  - Do not implement structured JSON logging (not in Python original)
  - Do not add features beyond what's listed above

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Tasks 2, 4, 5 after Task 1)
  - **Parallel Group**: Wave 1
  - **Blocks**: Tasks 6-28
  - **Blocked By**: Task 1

  **References**:
  - `dist/common/scripts/scylla_util.py:25-36` — `scylla_excepthook`: catches unhandled exceptions, dumps traceback with variables to `/var/tmp/scylla/`, uses `traceback_with_variables` library
  - Python's crash dump format: `{script}-{pid}-debug.log` in `/var/tmp/scylla/`
  - `dist/common/scripts/scylla_util.py:214-220` — `colorprint()` with ANSI colors (green, red, nocolor)

  **Acceptance Criteria**:
  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Crash context file created on panic
    Tool: Bash
    Steps:
      1. Build a test binary that panics intentionally
      2. Run it, expect non-zero exit
      3. ls /var/tmp/scylla/*-debug.log
      4. Assert log file exists and contains backtrace + args
    Expected Result: Debug log created with meaningful content
    Evidence: .sisyphus/evidence/task-3-crash-dump.txt

  Scenario: Verbose mode prints command execution
    Tool: Bash
    Steps:
      1. Run test binary with --verbose that calls out("echo hello")
      2. Assert stderr contains the command being run
    Expected Result: Verbose output shows commands
    Evidence: .sisyphus/evidence/task-3-verbose.txt

  Scenario: Dry-run mode prevents execution
    Tool: Bash
    Steps:
      1. Run test binary with --dry-run that would create a file
      2. Assert file does NOT exist
      3. Assert stdout shows what WOULD have happened
    Expected Result: No side effects, informative output
    Evidence: .sisyphus/evidence/task-3-dry-run.txt
  ```

  **Commit**: YES (groups with T2, T4, T5)
  - Message: `feat(scripts-rs): implement debug infrastructure — verbose, crash dumps, dry-run, diagnostics`
  - Files: `dist/common/scripts-rs/scylla-scripts-lib/src/debug.rs`

- [ ] 4. Lib Crate Config — sysconfig_parser, YAML Parsing, sysconfdir

  **What to do**:
  - Implement `config.rs`:
    - `SysconfigParser` struct — reads/writes shell-style KEY=VALUE files. Must handle: quoted values, escaped quotes, missing files (create empty), get/set/has_option/commit. Must match Python's `sysconfig_parser` exactly including quote escaping rules.
    - `parse_scylla_dirs_with_default(conf)` — reads scylla.yaml, applies defaults for workdir, data_file_directories, commitlog_directory, etc. Must match Python's defaulting logic exactly.
    - `get_scylla_dirs()` — returns list of scylla directories
    - `get_product(dir)` — reads SCYLLA-PRODUCT-FILE
    - `perftune_base_command()` — generates perftune.py command line from scylla dirs
  - Implement sysconfdir mechanism: read `scylla_sysconfdir.conf` (install-time generated key=value file with `SYSCONFDIR="..."`, matching Python's `scylla_sysconfdir.py` variable name). Overridable via `SCYLLA_SCRIPTS_TEST_SYSCONFDIR` env var for testability. Must be compatible with `install.sh`'s generation pattern (T29 generates this file).
  - Unit tests: parse real-world sysconfig files, YAML files with missing keys, edge cases (empty values, special chars)

  **Must NOT do**:
  - Do not change the sysconfig file format
  - Do not change YAML defaulting logic

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Tasks 2, 3, 5 after Task 1)
  - **Parallel Group**: Wave 1
  - **Blocks**: Tasks 6-28
  - **Blocked By**: Task 1

  **References**:
  - `dist/common/scripts/scylla_util.py:468-517` — `sysconfig_parser` class: __load (configparser hack), __escape/__unescape, __format_line, get/set/has_option/commit
  - `dist/common/scripts/scylla_util.py:223-261` — `parse_scylla_dirs_with_default()`, `get_scylla_dirs()`, `perftune_base_command()`
  - `dist/common/scripts/scylla_util.py:519-526` — `get_product()`
  - `dist/common/scripts/scylla_sysconfdir.py` — just `SYSCONFDIR="/etc/sysconfig"` — generated by install.sh
  - `install.sh:578-612` — how sysconfdir.py is generated with the configured path

  **Acceptance Criteria**:
  **QA Scenarios (MANDATORY):**
  ```
  Scenario: SysconfigParser round-trip
    Tool: Bash (cargo test)
    Steps:
      1. cargo test config::sysconfig -- --nocapture
      2. Tests: create file, set KEY="value with spaces", commit, re-read, assert get(KEY) matches
      3. Test escaped quotes: set KEY='value with "quotes"', verify round-trip
    Expected Result: Identical read-back for all value types
    Evidence: .sisyphus/evidence/task-4-sysconfig.txt

  Scenario: YAML dirs parsing with defaults
    Tool: Bash (cargo test)
    Steps:
      1. cargo test config::yaml_dirs -- --nocapture
      2. Test with minimal yaml (no workdir key) — assert defaults applied
      3. Test with full yaml — assert values preserved
    Expected Result: Same defaults as Python for missing keys
    Evidence: .sisyphus/evidence/task-4-yaml-dirs.txt
  ```

  **Commit**: YES (groups with T2, T3, T5)
  - Message: `feat(scripts-rs): implement config parsing — sysconfig, YAML, sysconfdir`
  - Files: `dist/common/scripts-rs/scylla-scripts-lib/src/config.rs`

- [ ] 5. Lib Crate System — systemd_unit Wrapper, pkg_install, Disk Utilities

  **What to do**:
  - Implement `systemd.rs`: `SystemdUnit` struct with start/stop/restart/enable/disable/mask/unmask/is_active/reload/available. Must handle nonroot mode (`--user` flag). Must raise `SystemdException` equivalent on invalid units.
  - Implement `pkg.rs`: `pkg_install(pkg, offline_exit)`, `pkg_uninstall(pkg)` dispatching to yum/apt/emerge/zypper based on distro detection. Must handle `pkg_xlat` table (package name translation across distros). Must handle `apt_is_updated()` cache freshness check and retry logic with lock waiting.
  - Implement `disk.rs`: `is_unused_disk(dev)`, `is_system_partition(dev)`, `get_partition_uuid(dev)`, `SYSTEM_PARTITION_UUIDS` constant, `is_valid_nic(nic)`, `swap_exists()`, `check_sysfs_numa_topology_is_valid()`.
  - Implement `color.rs`: `colorprint()` with ANSI escape codes (green, red, nocolor).
  - Unit tests for systemd (mock systemctl calls), pkg name translation table, disk utility logic.

  **Must NOT do**:
  - Do not actually call package managers in tests
  - Do not change package name translation table

  **Recommended Agent Profile**:
  - **Category**: `deep`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES (with Tasks 2, 3, 4 after Task 1)
  - **Parallel Group**: Wave 1
  - **Blocks**: Tasks 6-28
  - **Blocked By**: Task 1

  **References**:
  - `dist/common/scripts/scylla_util.py:419-465` — `systemd_unit` class with all methods + nonroot `--user` handling
  - `dist/common/scripts/scylla_util.py:305-392` — package management: `pkg_install`, `apt_install` (retry logic, lock waiting, 30 retries × 10s), `yum_install`, `emerge_install`, `zypper_install`, `pkg_xlat` table
  - `dist/common/scripts/scylla_util.py:186-211` — disk utilities: `SYSTEM_PARTITION_UUIDS`, `is_unused_disk`, `is_system_partition`
  - `dist/common/scripts/scylla_util.py:264-303` — `is_valid_nic`, `swap_exists`, `check_sysfs_numa_topology_is_valid`, `get_set_nic_and_disks_config_value`
  - `dist/common/scripts/scylla_util.py:214-220` — `colorprint` with ANSI CONCOLORS

  **Acceptance Criteria**:
  **QA Scenarios (MANDATORY):**
  ```
  Scenario: SystemdUnit validates unit existence
    Tool: Bash (cargo test)
    Steps:
      1. cargo test systemd:: -- --nocapture
      2. Test: construct SystemdUnit for nonexistent unit, assert error
      3. Test: construct for valid unit, assert methods callable
    Expected Result: Error on invalid unit, success on valid
    Evidence: .sisyphus/evidence/task-5-systemd.txt

  Scenario: Package name translation table matches Python
    Tool: Bash (cargo test)
    Steps:
      1. cargo test pkg::xlat -- --nocapture
      2. Assert cpupowerutils→linux-cpupower on debian
      3. Assert cpupowerutils→cpupower on suse
      4. Assert policycoreutils-python-utils→policycoreutils-python on amzn2
    Expected Result: All translations match Python's pkg_xlat dict
    Evidence: .sisyphus/evidence/task-5-pkg-xlat.txt
  ```

  **Commit**: YES (groups with T2, T3, T4)
  - Message: `feat(scripts-rs): implement system utilities — systemd, pkg management, disk utils`
  - Files: `dist/common/scripts-rs/scylla-scripts-lib/src/systemd.rs`, `pkg.rs`, `disk.rs`, `color.rs`

- [ ] 6. scylla_logrotate (17 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-logrotate/` bin crate
  - Logic: resolve `scylladir_p() / 'scylla-server.log'`, if exists and size > 0, rename to `scylla-server.log.{ISO-datetime}`
  - Add `DebugArgs` (--verbose, --dry-run, --diagnose) from lib crate
  - Integration test: create a fake log file, run binary, assert renamed with timestamp

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 2 (parallel with T7-T13). **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_logrotate` — complete source (17 lines), uses `scylladir_p()` and `datetime.today().isoformat()`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Log file rotated with timestamp
    Tool: Bash
    Steps:
      1. mkdir -p /tmp/test-scylla && echo "log" > /tmp/test-scylla/scylla-server.log
      2. Run binary (with env override for scylladir)
      3. Assert scylla-server.log no longer exists
      4. Assert scylla-server.log.{date} exists
    Expected Result: File renamed with ISO date suffix
    Evidence: .sisyphus/evidence/task-6-logrotate.txt

  Scenario: No-op when log empty or missing
    Tool: Bash
    Steps:
      1. Ensure no scylla-server.log exists
      2. Run binary, assert exit code 0, no files created
    Expected Result: Clean exit, no error
    Evidence: .sisyphus/evidence/task-6-logrotate-noop.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_logrotate Rust replacement`

- [ ] 7. scylla_fstrim_setup (19 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-fstrim-setup/` bin crate
  - Logic: check uid > 0 → exit 1 "Requires root permission.", then `systemd_unit('scylla-fstrim.timer').enable()` and `.start()`
  - Add DebugArgs

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 2. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_fstrim_setup` — complete source (19 lines)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Rejects non-root
    Tool: Bash
    Steps:
      1. Run as non-root user
      2. Assert exit code 1
      3. Assert stdout contains "Requires root permission."
    Expected Result: Permission check works
    Evidence: .sisyphus/evidence/task-7-fstrim-setup-nonroot.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_fstrim_setup Rust replacement`

- [ ] 8. scylla_stop (25 lines → Rust binary, systemd critical path!)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-stop/` bin crate
  - Logic: check root, read `sysconfig_parser(sysconfdir_p() / 'scylla-server')`, check NETWORK_MODE. If 'virtio' → `ip tuntap del mode tap dev {TAP}`. If 'dpdk' → unbind/rebind DPDK device.
  - Add DebugArgs
  - **CRITICAL**: This is ExecStopPost in scylla-server.service. Must handle gracefully when config values are missing (service may be stopping in degraded state).

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 2. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_stop` — complete source (25 lines), reads NETWORK_MODE, TAP, ETHPCIID, ETHDRV from sysconfig
  - `dist/common/scripts/scylla_util.py:468-517` — sysconfig_parser used here

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Clean exit when NETWORK_MODE is not virtio/dpdk
    Tool: Bash
    Steps:
      1. Create mock sysconfig with NETWORK_MODE=posix
      2. Run binary with mock sysconfdir
      3. Assert exit code 0, no commands run
    Expected Result: No-op for standard network mode
    Evidence: .sisyphus/evidence/task-8-stop-posix.txt

  Scenario: Rejects non-root
    Tool: Bash
    Steps:
      1. Run as non-root, assert exit code 1
    Expected Result: "Requires root permission."
    Evidence: .sisyphus/evidence/task-8-stop-nonroot.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_stop Rust replacement`

- [ ] 9. scylla_dev_mode_setup (30 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-dev-mode-setup/` bin crate
  - Logic: check root (skip if nonroot or container), argparse `--developer-mode INT` (required). If 0 → truncate dev-mode.conf. If 1 → write `DEV_MODE=--developer-mode=1` via sysconfig_parser.
  - Add DebugArgs

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 2. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_dev_mode_setup` — complete source (30 lines)
  - Uses `is_nonroot()`, `is_container()`, `etcdir()`, `sysconfig_parser`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Enable developer mode
    Tool: Bash
    Steps:
      1. Run with --developer-mode 1
      2. Read {etcdir}/scylla.d/dev-mode.conf
      3. Assert contains DEV_MODE=--developer-mode=1
    Expected Result: Config file written correctly
    Evidence: .sisyphus/evidence/task-9-devmode-enable.txt

  Scenario: Disable developer mode
    Tool: Bash
    Steps:
      1. Run with --developer-mode 0
      2. Assert dev-mode.conf is empty
    Expected Result: Config file truncated
    Evidence: .sisyphus/evidence/task-9-devmode-disable.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_dev_mode_setup Rust replacement`

- [ ] 10. scylla_rsyslog_setup (30 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-rsyslog-setup/` bin crate
  - Logic: check root, argparse `--remote-server` (required). If no port, append :1514. Write rsyslog config to `/etc/rsyslog.d/scylla.conf`, restart rsyslog.service.
  - Add DebugArgs

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 2. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_rsyslog_setup` — complete source (31 lines)
  - Writes: `if $programname == 'scylla' then @@{server};RSYSLOG_SyslogProtocol23Format\n`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_rsyslog_setup --help 2>&1) <(./target/release/scylla_rsyslog_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-10-rsyslog-help-diff.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_rsyslog_setup Rust replacement`

- [ ] 11. scylla_selinux_setup (29 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-selinux-setup/` bin crate
  - Logic: check root, check `is_redhat_variant()` (exit 0 if not). Run `sestatus`, check if SELinux not disabled → `setenforce 0`, write `SELINUX=disabled` to `/etc/sysconfig/selinux` via sysconfig_parser.
  - Add DebugArgs

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 2. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_selinux_setup` — complete source (29 lines)
  - Uses: `is_redhat_variant()`, `out('sestatus')`, `sysconfig_parser`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Skips on non-RedHat
    Tool: Bash
    Steps:
      1. Run on a debian system
      2. Assert exit code 0
      3. Assert stdout contains "only supports Red Hat variants"
    Expected Result: Clean skip on wrong distro
    Evidence: .sisyphus/evidence/task-11-selinux-skip.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_selinux_setup Rust replacement`

- [ ] 12. scylla_config_get.py (45 lines → Rust binary, standalone)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-config-get/` bin crate
  - **Installed name MUST be `scylla_config_get.py`** (with `.py` extension) — packaging scripts (`dist/debian/debian/scylla-conf.preinst`) call it by this exact path: `/opt/scylladb/scripts/scylla_config_get.py`
  - In T29: the Rust binary is installed as `$rprefix/scripts/.rust/scylla_config_get.py` and the selector wrapper at `$rprefix/scripts/scylla_config_get.py` dispatches to it
  - Logic: argparse `-c/--config` (default `/etc/scylla/scylla.yaml`), `-g/--get` (required). Read YAML, print value. Lists → one per line. Dicts → `key:value` per line. Scalars → print directly. Missing key → `"key 'X' not found"`, exit 1.
  - **Note**: This script does NOT import scylla_util — only uses PyYAML and argparse. Can use serde_yaml directly.
  - Add DebugArgs

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 2. **Blocked By**: T1 only. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_config_get.py` — complete source (45 lines)
  - Output format: lists → one value per line, dicts → `key:value` per line, scalars → plain print

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Read scalar value from YAML
    Tool: Bash
    Steps:
      1. echo "listen_address: 127.0.0.1" > /tmp/test.yaml
      2. Run: scylla_config_get -c /tmp/test.yaml -g listen_address
      3. Assert stdout is "127.0.0.1"
    Expected Result: Scalar value printed
    Evidence: .sisyphus/evidence/task-12-config-get-scalar.txt

  Scenario: Missing key exits 1
    Tool: Bash
    Steps:
      1. Run: scylla_config_get -c /tmp/test.yaml -g nonexistent
      2. Assert exit code 1
      3. Assert output contains "key 'nonexistent' not found"
    Expected Result: Error with correct message
    Evidence: .sisyphus/evidence/task-12-config-get-missing.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_config_get Rust replacement`

- [ ] 13. scylla-housekeeping (213 lines → Rust binary, standalone HTTP client)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-housekeeping/` bin crate
  - Logic: subcommands `version` and `--uuid` management. Makes HTTP requests to AWS API Gateway for version checking. Uses `configparser` for UUID storage, `pkg_resources.parse_version` for version comparison.
  - **Note**: Does NOT import scylla_util. Standalone. Uses multiprocessing (fork) for parallel HTTP requests.
  - Deps: `reqwest` (blocking) or `ureq` for HTTP, `semver` crate for version comparison (must match `pkg_resources.parse_version` ordering behavior).
  - Implement subcommands: `version --mode {i|c}` with `--repo-files` arg. Match Python's exact argparse interface — no new flags.
  - Add DebugArgs
  - For testability: support `SCYLLA_SCRIPTS_TEST_HOUSEKEEPING_URL` env var to override the hardcoded AWS API Gateway URL (test-only hook, not exposed as CLI flag)

  **Recommended Agent Profile**: **Category**: `unspecified-high`, **Skills**: []

  **Parallelization**: Wave 2. **Blocked By**: T1 only. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla-housekeeping` — complete source (213 lines)
  - Version check URL: `https://i6a5h9l1kl.execute-api.us-east-1.amazonaws.com/prod/check_version`
  - Uses `pkg_resources.parse_version` — need to verify `semver` crate produces identical ordering for ScyllaDB version strings
  - Uses `multiprocessing` with `fork` start method for parallel HTTP calls — Rust equivalent: `rayon` or `std::thread`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla-housekeeping --help 2>&1) <(./target/release/scylla-housekeeping --help 2>&1)
    Expected Result: Zero diff (or minimal whitespace diff from clap vs argparse)
    Evidence: .sisyphus/evidence/task-13-housekeeping-help.txt

  Scenario: Version check with mock server (via test env var)
    Tool: Bash
    Steps:
      1. Start a mock HTTP server (e.g., python3 -m http.server with fixture response)
      2. SCYLLA_SCRIPTS_TEST_HOUSEKEEPING_URL=http://localhost:PORT/check_version ./target/release/scylla-housekeeping version --mode i --repo-files /etc/apt/sources.list.d/scylla.list
      3. Assert output format matches Python's output format
    Expected Result: Correct version comparison and output using mock URL
    Evidence: .sisyphus/evidence/task-13-housekeeping-version.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla-housekeeping Rust replacement`

### Wave 3 — Mid-Complexity System Scripts (depends on Wave 1 shared lib)

- [ ] 14. scylla_kernel_check (37 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-kernel-check/` bin crate
  - Root check (uid == 0), abort with clear error if not root
  - Ensure `mkfs.xfs` is available — if not, use shared lib `pkg_install("xfsprogs")` (distro-aware)
  - Create temp file (64MB), `losetup` to create loop device, `mkfs.xfs` it, mount to tmpdir
  - Run `{bindir}/iotune --fs-check --fs-check-path {tmpdir}`, capture exit code
  - Cleanup: unmount, `losetup -d`, remove temp file (cleanup must run even on failure)
  - Print result: "This is a supported kernel" or "This is NOT a supported kernel" based on iotune exit
  - Add DebugArgs (--verbose, --dry-run, --diagnose)

  **Must NOT do**: Change output messages, add kernel version detection, modify iotune behavior

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 3. **Blocked By**: T1-T5 (shared lib). **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_kernel_check` — complete source (37 lines)
  - `dist/common/scripts/scylla_util.py:bindir()` lines 35-36 — path resolution for iotune
  - `dist/common/scripts/scylla_util.py:pkg_install()` lines 229-252 — distro-aware package install
  - Shared lib crate (T2-T5) — `RootCheck`, `DebugArgs`, `pkg_install()`, `bindir()`
  - Python uses `tempfile.mkdtemp()` and manual cleanup — Rust equivalent: `tempfile` crate with RAII

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Exit code parity (no argparse in Python original)
    Tool: Bash
    Steps:
      1. Run python3 dist/common/scripts/scylla_kernel_check 2>&1 as non-root; capture exit code
      2. Run ./target/release/scylla_kernel_check 2>&1 as non-root; capture exit code
      3. Assert both exit with same non-zero code and both print root-required error
    Expected Result: Same exit code and error message pattern
    Evidence: .sisyphus/evidence/task-14-kernel-check-parity.txt

  Scenario: --dry-run shows intended actions without executing
    Tool: Bash
    Steps:
      1. Run ./target/release/scylla_kernel_check --dry-run
      2. Assert output contains "would create temp file", "would run iotune"
      3. Assert no temp file created, no losetup run
    Expected Result: Actions described but not executed
    Evidence: .sisyphus/evidence/task-14-kernel-check-dryrun.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_kernel_check Rust replacement`

- [ ] 15. scylla_memory_setup (42 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-memory-setup/` bin crate
  - Root check
  - Argparse: `--lock-memory`, `--memory`, `--reserve-memory` (all optional strings)
  - Build `MEM_CONF` string from provided args (format: `"--lock-memory {val} --memory {val} --reserve-memory {val}"`)
  - Write to `{etcdir}/scylla.d/memory.conf` using `sysconfig_parser` (key `MEM_CONF`, file format: shell key=value)
  - Add DebugArgs

  **Must NOT do**: Validate memory values, change config format, add memory detection

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 3. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_memory_setup` — complete source (42 lines)
  - `dist/common/scripts/scylla_util.py:etcdir()` line 22 — `/etc/scylla` path
  - `dist/common/scripts/scylla_util.py` class `sysconfig_parser` lines 93-138 — read/write shell key=value config
  - Shared lib crate — `SysconfigParser`, `etcdir()`, `RootCheck`, `DebugArgs`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_memory_setup --help 2>&1) <(./target/release/scylla_memory_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-15-memory-setup-help.txt

  Scenario: Writes config file correctly
    Tool: Bash
    Steps:
      1. Create temp dir as fake etcdir, set SCYLLA_CONF env if applicable
      2. Run scylla_memory_setup --lock-memory 1 --memory 16G --reserve-memory 512M (with etcdir override)
      3. Read the generated memory.conf file
      4. Assert contains MEM_CONF="--lock-memory 1 --memory 16G --reserve-memory 512M"
    Expected Result: Config file contains exact expected key=value
    Evidence: .sisyphus/evidence/task-15-memory-setup-write.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_memory_setup Rust replacement`

- [ ] 16. scylla_cpuset_setup (46 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-cpuset-setup/` bin crate
  - Container-aware root check: if not container AND not root → error
  - Argparse: `--cpuset` (string), `--smp` (int)
  - Read/write `/etc/scylla.d/cpuset.conf` via `sysconfig_parser`
  - Set `CPUSET` key from `--cpuset`, set `NR_CPU` key from `--smp`
  - If cpuset value changed, remove `perftune.yaml` if it exists (force re-generation)
  - Add DebugArgs

  **Must NOT do**: Validate cpuset format, add CPU topology detection

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 3. **Blocked By**: T1-T5. **Blocks**: T25 (scylla_io_setup uses cpuset.conf), T27 (scylla_sysconfig_setup calls this).

  **References**:
  - `dist/common/scripts/scylla_cpuset_setup` — complete source (46 lines)
  - `dist/common/scripts/scylla_util.py:is_container()` lines 291-303 — container detection
  - `dist/common/scripts/scylla_util.py:is_nonroot()` lines 284-290 — nonroot mode
  - `dist/common/scripts/scylla_util.py` class `sysconfig_parser` lines 93-138
  - Shared lib crate — `SysconfigParser`, `is_container()`, `RootCheck`, `DebugArgs`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_cpuset_setup --help 2>&1) <(./target/release/scylla_cpuset_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-16-cpuset-setup-help.txt

  Scenario: Removes perftune.yaml when cpuset changes
    Tool: Bash
    Steps:
      1. Create fake etcdir with cpuset.conf containing CPUSET=0-3
      2. Create a dummy perftune.yaml
      3. Run scylla_cpuset_setup --cpuset 0-7
      4. Assert cpuset.conf now has CPUSET=0-7
      5. Assert perftune.yaml was deleted
    Expected Result: Config updated, perftune.yaml removed
    Evidence: .sisyphus/evidence/task-16-cpuset-change.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_cpuset_setup Rust replacement`

- [ ] 17. scylla_nofile_setup (43 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-nofile-setup/` bin crate
  - Nonroot-aware: if nonroot AND uid > 0, print "Requires root permission." and exit 1
  - Argparse: `--limitnofile` (optional int, "Specify LimitNOFILE size (default: auto-configure)")
  - If `--limitnofile` provided, use that value directly
  - Otherwise calculate: `limitnofile = 10000 + (1200 * mem_gb) + (10000 * cpu_count)` where `mem_gb = total_mem_bytes / 1024 / 1024 / 1024` (integer division), `cpu_count` from `/proc/cpuinfo` or `sysinfo`
  - If `limitnofile < 800000`: print "No need to enlarge LimitNOFILE, skipping setup." and exit 0
  - Otherwise print `Set LimitNOFILE to {limitnofile}`
  - Create dir `/etc/systemd/system/scylla-server.service.d/` (exist_ok)
  - Write `/etc/systemd/system/scylla-server.service.d/limitnofile.conf` with content: `[Service]\nLimitNOFILE={limitnofile}`
  - Call `systemd_unit.reload()` (systemctl daemon-reload)
  - Add DebugArgs

  **Must NOT do**: Change the calculation formula, change the 800000 threshold, add custom systemd unit name

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 3. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_nofile_setup` — complete source (43 lines)
  - Line 30: formula `10000 + (1200 * mem_gb) + (10000 * cpu)`
  - Line 31: threshold `< 800000` → skip
  - `dist/common/scripts/scylla_util.py` class `systemd_unit` lines 140-226 — reload method
  - `dist/common/scripts/scylla_util.py:is_nonroot()` lines 284-290
  - Python uses `psutil.cpu_count()` and `int(psutil.virtual_memory().total/1024/1024/1024)` — Rust: `/proc/cpuinfo` + `/proc/meminfo`
  - Shared lib crate — `SystemdUnit`, `is_nonroot()`, `DebugArgs`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python (filtered for debug flags)
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_nofile_setup --help 2>&1) <(./target/release/scylla_nofile_setup --help 2>&1 | grep -v -E '^\s*--(verbose|dry-run|diagnose)')
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-17-nofile-help.txt

  Scenario: Calculation matches Python on same system
    Tool: Bash
    Steps:
      1. Get cpu_count and mem_gb: python3 -c "import psutil; print(psutil.cpu_count(), int(psutil.virtual_memory().total/1024/1024/1024))"
      2. Compute expected: 10000 + 1200*mem_gb + 10000*cpu_count
      3. Run ./target/release/scylla_nofile_setup --dry-run --verbose
      4. Assert verbose output shows same cpu/mem values and same computed limitnofile
    Expected Result: Identical computation
    Evidence: .sisyphus/evidence/task-17-nofile-calc.txt

  Scenario: Skips when below threshold
    Tool: Bash
    Steps:
      1. Run ./target/release/scylla_nofile_setup --limitnofile 100000 --dry-run
      2. Assert output contains "No need to enlarge LimitNOFILE, skipping setup."
      3. Assert exit code 0
    Expected Result: Skips correctly
    Evidence: .sisyphus/evidence/task-17-nofile-skip.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_nofile_setup Rust replacement`

- [ ] 18. scylla_fstrim (46 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-fstrim/` bin crate
  - Parse scylla.yaml to get data/commitlog dirs via `parse_scylla_dirs_with_default()`
  - For each dir, resolve to mount point (parse `/proc/mounts`)
  - Deduplicate mount points
  - Run `scylla-blocktune --set-nomerges 1` on each mount's device
  - Run `fstrim {mountpoint}` on each
  - Run `scylla-blocktune --set-nomerges 2` on each mount's device
  - Add DebugArgs

  **Must NOT do**: Change nomerges values (1 then 2), add scheduling, modify blocktune behavior

  **Recommended Agent Profile**: **Category**: `quick`, **Skills**: []

  **Parallelization**: Wave 3. **Blocked By**: T1-T5 (shared lib), effectively also T23 (scylla_blocktune binary must exist). **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_fstrim` — complete source (46 lines)
  - `dist/common/scripts/scylla_util.py:parse_scylla_dirs_with_default()` lines 306-348 — YAML parsing for data dirs
  - `dist/common/scripts/scylla_blocktune.py:tune_fs()` — called via subprocess `scylla-blocktune`
  - Shared lib crate — `parse_scylla_dirs_with_default()`, `DebugArgs`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_fstrim --help 2>&1) <(./target/release/scylla_fstrim --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-18-fstrim-help.txt

  Scenario: --dry-run shows blocktune and fstrim commands without executing
    Tool: Bash
    Steps:
      1. Create minimal scylla.yaml with data_file_directories pointing to /tmp/testdir
      2. Run scylla_fstrim --dry-run
      3. Assert output shows "would run: scylla-blocktune --set-nomerges 1 ..." and "would run: fstrim ..."
    Expected Result: Commands listed, not executed
    Evidence: .sisyphus/evidence/task-18-fstrim-dryrun.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_fstrim Rust replacement`

- [ ] 19. scylla_cpuscaling_setup (98 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-cpuscaling-setup/` bin crate
  - Root check
  - Heavy distro branching — this is where shared lib distro detection pays off:
    - Debian/Ubuntu: `apt-get install cpufrequtils`, write `GOVERNOR="performance"` to `/etc/default/cpufrequtils`, restart `cpufrequtils` service
    - Gentoo: write `cpufreq_governor="performance"` to `/etc/conf.d/cpupower`
    - Arch: write `governor='performance'` to `/etc/default/cpupower`
    - Amazon Linux 2: write systemd unit `scylla-cpupower.service` that runs `cpupower frequency-set -g performance`, install `kernel-tools`, enable+start
    - SUSE: write systemd unit `scylla-cpupower.service`, install `cpupower`, enable+start
    - RHEL/CentOS/Fedora: `yum install cpupowerutils`, `cpupower frequency-set -g performance`, write `/etc/sysconfig/cpupower` with `CPUPOWER_START_OPTS="frequency-set -g performance"`, enable+start `cpupower`
  - Add DebugArgs

  **Must NOT do**: Add governor detection, support governors other than "performance", modify existing systemd units

  **Recommended Agent Profile**: **Category**: `unspecified-high`, **Skills**: []
  - Reason: Complex distro branching, multiple config file formats, systemd unit creation

  **Parallelization**: Wave 3. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_cpuscaling_setup` — complete source (98 lines)
  - `dist/common/scripts/scylla_util.py` distro detection: `is_debian_variant()` line 61, `is_redhat_variant()` line 73, `is_suse_variant()` line 83, `is_gentoo()` line 87, `is_arch()` line 91, `is_amzn2()` line 95
  - `dist/common/scripts/scylla_util.py` class `systemd_unit` lines 140-226 — enable/start/restart
  - `dist/common/scripts/scylla_util.py:pkg_install()` lines 229-252
  - `dist/common/scripts/scylla_util.py` class `sysconfig_parser` lines 93-138
  - Shared lib crate — all distro detection, `SystemdUnit`, `SysconfigParser`, `pkg_install()`, `RootCheck`, `DebugArgs`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_cpuscaling_setup --help 2>&1) <(./target/release/scylla_cpuscaling_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-19-cpuscaling-help.txt

  Scenario: --dry-run on current system shows correct distro branch
    Tool: Bash
    Steps:
      1. Run scylla_cpuscaling_setup --dry-run --verbose
      2. Assert output shows detected distro and the specific commands/files it would use
      3. Assert no packages installed, no files written
    Expected Result: Correct distro detected, commands shown, nothing executed
    Evidence: .sisyphus/evidence/task-19-cpuscaling-dryrun.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_cpuscaling_setup Rust replacement`

- [ ] 20. scylla_ntp_setup (118 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-ntp-setup/` bin crate
  - Root check
  - Detect NTP system: check `systemd-timesyncd`, `chronyd`, `ntpd` availability (via `systemd_unit.available()`)
  - If none found: install `ntp` (redhat) or `ntp` (debian) via `pkg_install`
  - Configure NTP subdomain: for chrony edit `/etc/chrony.conf`, for ntpd edit `/etc/ntp.conf`, for timesyncd edit `/etc/systemd/timesyncd.conf`
  - **Known bug in Python**: lines 92 and 109 use undefined `confpath` variable — Rust version should FIX this bug (use correct path variable)
  - Enable and start the detected NTP service
  - Add DebugArgs

  **Must NOT do**: Add NTP server configuration, change default NTP subdomain, add time sync verification

  **Recommended Agent Profile**: **Category**: `unspecified-high`, **Skills**: []

  **Parallelization**: Wave 3. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_ntp_setup` — complete source (118 lines)
  - Lines 92, 109 — **BUG**: `confpath` used but never defined; should be `conf` (the variable assigned on lines 83, 97)
  - `dist/common/scripts/scylla_util.py:is_redhat_variant()` line 73
  - `dist/common/scripts/scylla_util.py` class `systemd_unit` lines 140-226 — available/enable/start
  - `dist/common/scripts/scylla_util.py:pkg_install()` lines 229-252
  - Shared lib crate — `SystemdUnit`, `pkg_install()`, `is_redhat_variant()`, `RootCheck`, `DebugArgs`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_ntp_setup --help 2>&1) <(./target/release/scylla_ntp_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-20-ntp-help.txt

  Scenario: --dry-run detects NTP system and shows intended actions
    Tool: Bash
    Steps:
      1. Run scylla_ntp_setup --dry-run --verbose
      2. Assert output shows which NTP system was detected
      3. Assert shows config file path and what would be written
    Expected Result: NTP system detected, actions described, nothing modified
    Evidence: .sisyphus/evidence/task-20-ntp-dryrun.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_ntp_setup Rust replacement`

- [ ] 21. scylla_sysconfig_setup (130 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-sysconfig-setup/` bin crate
  - Root check
  - Argparse with many options: `--nic`, `--mode` (posix/dpdk/virtio), `--nr-hugepages`, `--user`, `--group`, `--homedir`, `--confdir`, `--setup-nic-and-disks`, `--set-clocksource`, `--disable-writeback-cache`
  - Read scylla-server sysconfig file via `sysconfig_parser`
  - For each provided arg, update the corresponding sysconfig key
  - If `--setup-nic-and-disks` or `--set-clocksource` or `--disable-writeback-cache`: call `perftune_base_command()` to get CPU mask, then call `scylla_cpuset_setup` as subprocess
  - DPDK mode: additional hugepages setup, run `dpdk_setup.py` (but note: DPDK is legacy/deprecated)
  - Add DebugArgs

  **Must NOT do**: Add new config options, change sysconfig key names, modify perftune behavior

  **Recommended Agent Profile**: **Category**: `unspecified-high`, **Skills**: []

  **Parallelization**: Wave 3. **Blocked By**: T1-T5, T16 (calls scylla_cpuset_setup). **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_sysconfig_setup` — complete source (130 lines)
  - `dist/common/scripts/scylla_util.py:perftune_base_command()` lines 256-282 — builds perftune.py command
  - `dist/common/scripts/scylla_util.py:get_set_nic_and_disks_config_value()` lines 370-397
  - `dist/common/scripts/scylla_util.py:hex2list()` lines 351-369 — CPU mask conversion
  - `dist/common/scripts/scylla_util.py` class `sysconfig_parser` lines 93-138
  - Shared lib crate — `SysconfigParser`, `perftune_base_command()`, `hex2list()`, `RootCheck`, `DebugArgs`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_sysconfig_setup --help 2>&1) <(./target/release/scylla_sysconfig_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-21-sysconfig-help.txt

  Scenario: --dry-run with --nic eth0 --mode posix shows config changes
    Tool: Bash
    Steps:
      1. Create fake sysconfig file with existing values
      2. Run scylla_sysconfig_setup --nic eth0 --mode posix --dry-run --verbose
      3. Assert shows what keys would be updated without writing
    Expected Result: Changes shown, file not modified
    Evidence: .sisyphus/evidence/task-21-sysconfig-dryrun.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_sysconfig_setup Rust replacement`

- [ ] 22. scylla_swap_setup (147 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-swap-setup/` bin crate
  - Root check
  - Check if swap already exists via `swap_exists()` (parse `/proc/swaps`) — if yes, exit 0
  - Auto-size: `min(16 * 1024^3, memtotal / 3)` — get memtotal from `/proc/meminfo` (Python uses `psutil.virtual_memory().total`)
  - Create swapfile at `/var/lib/scylla/swap/swapfile`: `fallocate`, `mkswap`, `swapon`
  - Create systemd swap unit at `/etc/systemd/system/var-lib-scylla-swap-swapfile.swap` with `DefaultDependencies=no` (Azure workaround — swap must activate before mounts)
  - Enable the swap unit via `systemd_unit`
  - Add DebugArgs

  **Must NOT do**: Change swap sizing formula, add custom swap size flag, modify Azure workaround

  **Recommended Agent Profile**: **Category**: `unspecified-high`, **Skills**: []

  **Parallelization**: Wave 3. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_swap_setup` — complete source (147 lines)
  - `dist/common/scripts/scylla_util.py:swap_exists()` lines 268-271 — checks `/proc/swaps`
  - `dist/common/scripts/scylla_util.py` class `systemd_unit` lines 140-226
  - Python uses `psutil.virtual_memory().total` — Rust: parse `/proc/meminfo` for `MemTotal:`
  - Python systemd unit template: lines 62-73 of scylla_swap_setup
  - Azure workaround comment: lines 56-60 — `DefaultDependencies=no` is critical

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_swap_setup --help 2>&1) <(./target/release/scylla_swap_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-22-swap-help.txt

  Scenario: --dry-run shows swap sizing calculation
    Tool: Bash
    Steps:
      1. Run scylla_swap_setup --dry-run --verbose
      2. Assert output shows detected memtotal, calculated swap size
      3. Assert shows path where swapfile would be created
      4. Assert no file created, no mkswap run
    Expected Result: Calculations shown, nothing executed
    Evidence: .sisyphus/evidence/task-22-swap-dryrun.txt

  Scenario: Exits cleanly if swap already exists
    Tool: Bash
    Steps:
      1. Verify system has swap (or mock /proc/swaps)
      2. Run scylla_swap_setup --verbose
      3. Assert output mentions swap already exists
      4. Assert exit code 0
    Expected Result: Clean exit, no action taken
    Evidence: .sisyphus/evidence/task-22-swap-exists.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_swap_setup Rust replacement`

### Wave 4 — Complex Scripts (depends on Waves 1-3, especially shared lib and blocktune)

- [ ] 23. scylla_blocktune — dual-mode: library module + CLI binary (89 lines → Rust lib+bin)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-blocktune/` crate as BOTH lib and bin (Cargo supports `[lib]` + `[[bin]]` in same crate)
  - **Library functions** (used by scylla_io_setup T25): `tune_path()`, `tune_blockdev()`, `tune_dev()`, `tune_fs()`, `tune_yaml()`
  - `tune_blockdev(path)`: resolve path→mount→device, write scheduler/nomerges/add_random sysfs tunables via `try_write()`
  - `tune_dev(device)`: write sysfs tunables directly for a device
  - `tune_fs(path, nomerges_val)`: resolve path→mount→device, write nomerges value
  - `tune_yaml()`: calls `parse_scylla_dirs_with_default()`, tunes all data/commitlog dirs
  - **CLI binary** `scylla-blocktune` (separate 34-line entrypoint): argparse with `--config YAML` (process scylla.yaml, append), `--filesystem PATH` (tune filesystem at PATH, append), `--dev PATH` (tune device node, append), `--set-nomerges VAL` (overwrite nomerges parameter)
  - Default behavior (no args): equivalent to `--config /etc/scylla/scylla.yaml`
  - `try_write()`: writes to sysfs, catches permission errors and prints warning (not fatal)
  - Add DebugArgs to CLI

  **Must NOT do**: Change sysfs tunable values, add new tunables, modify tune logic

  **Recommended Agent Profile**: **Category**: `deep`, **Skills**: []
  - Reason: Dual lib+bin architecture, sysfs interaction, used as dependency by T25

  **Parallelization**: Wave 4. **Blocked By**: T1-T5 (shared lib). **Blocks**: T25 (scylla_io_setup imports as module), T18 (scylla_fstrim calls as binary).

  **References**:
  - `dist/common/scripts/scylla_blocktune.py` — complete source (89 lines)
  - `dist/common/scripts/scylla_util.py:parse_scylla_dirs_with_default()` lines 306-348
  - Python resolves block device from path via: `os.stat(path).st_dev` → `os.major/minor` → `/sys/dev/block/{maj}:{min}` → readlink → device name
  - Sysfs tunables written: `/sys/block/{dev}/queue/scheduler` (none), `/sys/block/{dev}/queue/nomerges` (2), `/sys/block/{dev}/queue/add_random` (0)
  - Shared lib crate — `parse_scylla_dirs_with_default()`, `DebugArgs`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: CLI --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla-blocktune --help 2>&1) <(./target/release/scylla-blocktune --help 2>&1 | grep -v -E '^\s*--(verbose|dry-run|diagnose)')
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-23-blocktune-help.txt

  Scenario: Library API is public and usable
    Tool: Bash
    Steps:
      1. Verify scylla-blocktune crate's lib.rs exposes pub functions: tune_path, tune_yaml, tune_dev, tune_fs
      2. Write a small Rust test in scylla-blocktune's own test module that calls each public function signature
      3. cargo test -p scylla-blocktune -- confirms lib API compiles and is accessible
    Expected Result: Public API verified via crate's own tests
    Evidence: .sisyphus/evidence/task-23-blocktune-lib.txt

  Scenario: --dry-run --config shows what would be tuned
    Tool: Bash
    Steps:
      1. Create minimal scylla.yaml with data dirs
      2. Run scylla-blocktune --config /path/to/test-scylla.yaml --dry-run --verbose
      3. Assert output lists devices and tunables that would be modified
    Expected Result: Devices and tunables listed, nothing written
    Evidence: .sisyphus/evidence/task-23-blocktune-dryrun.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_blocktune Rust replacement (lib+bin)`

- [ ] 24. scylla_coredump_setup (199 lines → Rust binary)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-coredump-setup/` bin crate
  - Root check
  - Argparse: `--dump-to-raiddir` (flag), `--compress` (flag)
  - Stop `abrt-ccpp` service if running (RHEL)
  - RHEL9 SELinux workaround: if RHEL9, run `semanage permissive -a systemd_coredump_t` (install `policycoreutils-python-utils` if needed)
  - Sysctl: set `kernel.core_pattern=|/usr/lib/systemd/systemd-coredump %P %u %g %s %t %c %h %e` via `/etc/sysctl.d/99-scylla-coredump.conf` (Gentoo: `/etc/sysctl.d/`)
  - Write `/etc/systemd/coredump.conf`: `Storage=external`, `Compress=yes`, `ProcessSizeMax=infinity`, `ExternalSizeMax=infinity`
  - If `--dump-to-raiddir`: create bind mount from `/var/lib/systemd/coredump` to raid data dir, create systemd `.mount` unit
  - Test coredump: run `sleep 1000000 &`, kill with SIGABRT, wait, run `coredumpctl info` and verify output
  - Parse `coredumpctl` output for different systemd versions (field name varies)
  - Add DebugArgs

  **Must NOT do**: Change coredump.conf values, add coredump retention policy, modify SELinux beyond the specific workaround

  **Recommended Agent Profile**: **Category**: `deep`, **Skills**: []
  - Reason: Complex systemd interaction, SELinux workaround, coredump testing, multiple distro paths

  **Parallelization**: Wave 4. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_coredump_setup` — complete source (199 lines)
  - Lines 27-38: RHEL9 SELinux workaround — detect via `distro.id()` + version check
  - Lines 40-57: sysctl configuration — Gentoo uses different path
  - Lines 59-83: coredump.conf writing
  - Lines 85-128: `--dump-to-raiddir` bind mount logic with systemd mount unit
  - Lines 130-199: coredump test — spawns sleep, kills with SIGABRT, parses coredumpctl output
  - `dist/common/scripts/scylla_util.py:is_redhat_variant()`, `is_gentoo()`
  - `dist/common/scripts/scylla_util.py` class `systemd_unit` — stop abrt-ccpp
  - Python uses `distro.id()` for RHEL9 detection — Rust: parse `/etc/os-release`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_coredump_setup --help 2>&1) <(./target/release/scylla_coredump_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-24-coredump-help.txt

  Scenario: --dry-run shows all configuration steps
    Tool: Bash
    Steps:
      1. Run scylla_coredump_setup --dry-run --verbose
      2. Assert output shows: sysctl file path+content, coredump.conf path+content
      3. If RHEL detected, assert shows SELinux workaround
      4. Assert nothing written to filesystem
    Expected Result: Full action plan shown, nothing modified
    Evidence: .sisyphus/evidence/task-24-coredump-dryrun.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_coredump_setup Rust replacement`

- [ ] 25. scylla_io_setup (228 lines → Rust binary, imports blocktune as lib)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-io-setup/` bin crate, add `scylla-blocktune` as Cargo dependency
  - `ScyllaCpuinfo` struct: parse `/etc/scylla.d/cpuset.conf` for CPUSET, parse `/proc/cpuinfo` for processor list, intersect to get active CPUs
  - Root check
  - `run_iotune()`:
    - Parse scylla.yaml for data dirs via `parse_scylla_dirs_with_default()`
    - Validate all data dirs exist
    - Call `blocktune::tune_yaml()` (library call, not subprocess)
    - Get active CPU list from `ScyllaCpuinfo`
    - Set RLIMIT_NOFILE to 200000 (Rust: `setrlimit`)
    - Run `{bindir}/iotune --evaluation-directory {datadir[0]} --format seastar-io-request-format ...` with CPU pinning
    - Write result to `/etc/scylla.d/io.conf` and `io_properties.yaml`
  - AWS 4K sector workaround: detect AWS instance type via IMDSv2 (`http://169.254.169.254/latest/meta-data/instance-type`), if i3/i3en, add `--storage-classes 4096:1` to iotune args
  - Add DebugArgs

  **Must NOT do**: Change iotune flags, modify blocktune tuning values, add disk benchmarking beyond iotune

  **Recommended Agent Profile**: **Category**: `deep`, **Skills**: []
  - Reason: Imports blocktune as library, resource limits, AWS detection, iotune subprocess management

  **Parallelization**: Wave 4. **Blocked By**: T1-T5, T16 (cpuset.conf), T23 (blocktune lib). **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_io_setup` — complete source (228 lines)
  - `dist/common/scripts/scylla_blocktune.py` — imported as `import scylla_blocktune` on line 6
  - Lines 10-34: `scylla_cpuinfo` class — cpuset.conf + /proc/cpuinfo parsing
  - Lines 36-95: `run_iotune()` — main logic
  - Lines 97-130: AWS instance type detection via IMDSv2 with TOKEN
  - `dist/common/scripts/scylla_util.py:parse_scylla_dirs_with_default()` lines 306-348
  - `dist/common/scripts/scylla_util.py:bindir()` line 35
  - Python uses `resource.setrlimit(resource.RLIMIT_NOFILE, (200000, 200000))` — Rust: `libc::setrlimit`

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_io_setup --help 2>&1) <(./target/release/scylla_io_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-25-io-setup-help.txt

  Scenario: --dry-run shows iotune command that would be run
    Tool: Bash
    Steps:
      1. Create minimal scylla.yaml and cpuset.conf
      2. Run scylla_io_setup --dry-run --verbose
      3. Assert output shows: data dirs found, CPUs detected, iotune command with full args
      4. Assert iotune NOT actually run
    Expected Result: Full iotune command shown, nothing executed
    Evidence: .sisyphus/evidence/task-25-io-setup-dryrun.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_io_setup Rust replacement`

- [ ] 26. scylla_raid_setup (425 lines → Rust binary, most complex leaf script)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-raid-setup/` bin crate
  - Root check
  - Argparse: `--disks` (comma-separated device list), `--raiddev` (default `/dev/md0`), `--volume-role` (data/commitlog/all), `--update-fstab`, `--raid-level` (0/5), `--online-discard`
  - `UdevInfo` class → struct: query device properties via `libudev` (Rust `udev` crate). Multiple strategies for resolving device links: by-id, by-path, by-partlabel, fallback.
  - RAID creation: `mdadm --create` with calculated chunk size (1024K for RAID0 data, 64K for commitlog)
  - XFS formatting: `mkfs.xfs` with kernel-version-aware block size (4096 default, 1024 for kernel ≤ 5.3 due to XFS bug)
  - Create systemd mount unit (`.mount` file) for the RAID device
  - SELinux: `restorecon -R` on RHEL/variants for the mount point
  - `--update-fstab`: add entry to `/etc/fstab`
  - Cleanup: if RAID exists on target device, prompt or fail
  - Add DebugArgs

  **Must NOT do**: Add disk discovery, change RAID chunk sizes, modify XFS format options, add LVM support

  **Recommended Agent Profile**: **Category**: `deep`, **Skills**: []
  - Reason: Most complex script. Udev, mdadm, mkfs.xfs, systemd mounts, SELinux, kernel version detection.

  **Parallelization**: Wave 4. **Blocked By**: T1-T5. **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_raid_setup` — complete source (425 lines)
  - Lines 15-70: `UdevInfo` class — multiple device link resolution strategies
  - Lines 72-130: RAID creation — `mdadm --create` with chunk size logic
  - Lines 132-180: XFS formatting — kernel version check for block size
  - Lines 182-250: systemd mount unit creation
  - Lines 252-310: `--update-fstab` logic
  - Lines 312-360: SELinux relabeling (RHEL only)
  - Lines 362-425: argument parsing and main orchestration
  - `dist/common/scripts/scylla_util.py:is_redhat_variant()`, `is_unused_disk()` lines 264-267
  - Python uses `pyudev` — Rust: `udev` crate (libudev bindings)
  - Python uses `psutil.disk_partitions()` — Rust: parse `/proc/mounts`
  - Kernel version for XFS bug: `platform.release()` → Rust: `uname` syscall

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_raid_setup --help 2>&1) <(./target/release/scylla_raid_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-26-raid-help.txt

  Scenario: --dry-run with --disks /dev/sdb shows full plan
    Tool: Bash
    Steps:
      1. Run scylla_raid_setup --disks /dev/sdb --dry-run --verbose
      2. Assert output shows: device info from udev, mdadm command, mkfs.xfs command with block size, mount unit content
      3. Assert nothing executed (no mdadm, no mkfs)
    Expected Result: Full RAID creation plan shown
    Evidence: .sisyphus/evidence/task-26-raid-dryrun.txt

  Scenario: UdevInfo resolution compiles and handles missing device gracefully
    Tool: Bash
    Steps:
      1. Run scylla_raid_setup --disks /dev/nonexistent --verbose 2>&1
      2. Assert error message is clear and includes device name
      3. Assert exit code non-zero
    Expected Result: Clear error, no crash/panic
    Evidence: .sisyphus/evidence/task-26-raid-nodev.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_raid_setup Rust replacement`

- [ ] 27. scylla_prepare (190 lines → Rust binary, systemd ExecStartPre)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-prepare/` bin crate
  - This runs as `ExecStartPre` in scylla-server.service — must be reliable and fast
  - CPU feature check (x86_64 only): verify `sse4_2` and `pclmulqdq` in `/proc/cpuinfo` flags, abort with clear error if missing
  - Network mode handling: read sysconfig for `NETWORK_MODE`, handle posix/virtio/dpdk
  - Create `perftune.yaml` using `hwloc-calc` for IRQ CPU mask:
    - Get CPUSET from cpuset.conf
    - Run `hwloc-calc --taskset {cpuset}` to get hex mask
    - Convert hex mask to CPU list
    - Format perftune.yaml with nic, irq_cpu_mask, mode, tune settings
  - Call `perftune.py --options-file perftune.yaml` as subprocess
  - Handle nonroot mode (skip perftune)
  - Add DebugArgs (but note: --dry-run is critical here since this runs at service start)

  **Must NOT do**: Change CPU feature requirements, modify perftune.yaml format, add health checks

  **Recommended Agent Profile**: **Category**: `deep`, **Skills**: []
  - Reason: Service startup critical path. hwloc interaction, CPU feature detection, perftune integration.

  **Parallelization**: Wave 4. **Blocked By**: T1-T5, T16 (cpuset.conf), T21 (sysconfig). **Blocks**: T28.

  **References**:
  - `dist/common/scripts/scylla_prepare` — complete source (190 lines)
  - Lines 14-33: CPU feature verification — checks `/proc/cpuinfo` flags
  - Lines 35-80: Network mode + perftune.yaml creation
  - Lines 82-130: `hwloc-calc` invocation and hex→list conversion
  - Lines 132-165: perftune.py invocation
  - Lines 167-190: main flow with nonroot/container checks
  - `dist/common/scripts/scylla_util.py:hex2list()` lines 351-369
  - `dist/common/scripts/scylla_util.py:perftune_base_command()` lines 256-282
  - `dist/common/scripts/scylla_util.py:is_nonroot()`, `is_developer_mode()`
  - `dist/common/scripts/scylla_util.py` class `sysconfig_parser` — reads sysconfig for NETWORK_MODE

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: Exit-code parity (no argparse in Python original)
    Tool: Bash
    Steps:
      1. Run python3 dist/common/scripts/scylla_prepare 2>&1 as non-root; capture exit code and stderr
      2. Run ./target/release/scylla_prepare 2>&1 as non-root; capture exit code and stderr
      3. Assert both produce same exit code and similar error output
    Expected Result: Same exit behavior
    Evidence: .sisyphus/evidence/task-27-prepare-parity.txt

  Scenario: CPU feature check passes on current system
    Tool: Bash
    Steps:
      1. Run scylla_prepare --dry-run --verbose
      2. Assert output shows CPU feature check results (sse4_2, pclmulqdq)
      3. Assert shows what perftune.yaml would contain
    Expected Result: Features detected, perftune config shown
    Evidence: .sisyphus/evidence/task-27-prepare-dryrun.txt

  Scenario: CPU feature check fails gracefully on missing feature (unit test)
    Tool: Bash
    Steps:
      1. cargo test -p scylla-prepare -- cpu_feature
      2. Assert test covers case where feature is missing → returns specific error
    Expected Result: Unit test passes, error message is clear
    Evidence: .sisyphus/evidence/task-27-prepare-cpucheck.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_prepare Rust replacement`

### Wave 5 — Orchestrator (depends on ALL leaf scripts from Waves 2-4)

- [ ] 28. scylla_setup — interactive/non-interactive orchestrator (568 lines → Rust binary, LAST script migrated)

  **What to do**:
  - Create `dist/common/scripts-rs/scylla-setup/` bin crate
  - **This must be the LAST script migrated** — it calls nearly every other script as subprocess
  - `run_setup_script(name)`: run `{scriptsdir}/{name}` as subprocess, check exit code, abort on failure
  - Interactive mode (default): yes/no prompts for each setup step using `dialoguer` crate or manual stdin
  - Non-interactive mode (`--no-*` flags): skip specific steps based on CLI flags
  - Setup steps in order:
    1. `scylla_ntp_setup` (unless `--no-ntp-setup`)
    2. `scylla_raid_setup` (unless `--no-raid-setup`) — interactive: prompt for disks, show `lsblk` output
    3. `scylla_io_setup` (unless `--no-io-setup`)
    4. `scylla_cpuscaling_setup` (unless `--no-cpuscaling-setup`)
    5. `scylla_fstrim_setup`
    6. `scylla_coredump_setup` (unless `--no-coredump-setup`)
    7. `scylla_swap_setup` (unless `--no-swap-setup`)
    8. `scylla_sysconfig_setup` with collected args
    9. `scylla_memory_setup` if memory args provided
    10. `scylla_cpuset_setup` if cpuset args provided
    11. `scylla_kernel_check`
    12. `scylla-housekeeping version`
    13. `systemctl enable scylla-server`
  - Argparse: extensive — `--disks`, `--nic`, `--setup-nic-and-disks`, `--ntp-domain`, `--no-*` flags for each step, `--mode`, `--io-setup`, plus memory/cpuset passthrough args
  - At end of interactive mode: print the equivalent non-interactive command for reproducibility
  - At end: print "ScyllaDB setup is now complete" + housekeeping version check message
  - Add DebugArgs (--verbose propagated to sub-scripts, --dry-run propagated)

  **Must NOT do**: Change step order, add new setup steps, modify interactive prompt text, consolidate sub-script calls into inline code

  **Recommended Agent Profile**: **Category**: `deep`, **Skills**: []
  - Reason: Orchestrator calling 13+ scripts, interactive I/O, complex arg passthrough, must be last.

  **Parallelization**: Wave 5. **Blocked By**: ALL of T6-T27 (calls them as subprocesses). **Blocks**: T29.

  **References**:
  - `dist/common/scripts/scylla_setup` — complete source (568 lines)
  - Lines 1-30: imports — nearly everything from scylla_util
  - Lines 32-70: `run_setup_script()` helper + interactive yes/no prompt
  - Lines 72-180: argument parsing — ~30 flags
  - Lines 182-400: interactive mode flow — each step with prompt
  - Lines 402-500: non-interactive mode — direct calls based on flags
  - Lines 502-540: disk selection UI in interactive mode — `lsblk` + `is_unused_disk()` + prompt
  - Lines 542-568: completion message + non-interactive command echo
  - `dist/common/scripts/scylla_util.py:scriptsdir()` line 38 — path to scripts directory
  - `dist/common/scripts/scylla_util.py:is_unused_disk()` lines 264-267
  - All scripts T6-T27 — scylla_setup calls each as subprocess

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: --help matches Python
    Tool: Bash
    Steps:
      1. diff <(python3 dist/common/scripts/scylla_setup --help 2>&1) <(./target/release/scylla_setup --help 2>&1)
    Expected Result: Zero diff
    Evidence: .sisyphus/evidence/task-28-setup-help.txt

  Scenario: --dry-run with all --no-* flags shows step sequence
    Tool: Bash
    Steps:
      1. Run scylla_setup --no-ntp-setup --no-raid-setup --no-io-setup --no-cpuscaling-setup --no-coredump-setup --no-swap-setup --dry-run --verbose
      2. Assert output shows which steps would be skipped and which would run
      3. Assert no sub-scripts actually invoked
    Expected Result: Step plan shown, nothing executed
    Evidence: .sisyphus/evidence/task-28-setup-dryrun.txt

  Scenario: Non-interactive mode echoes reproducible command
    Tool: Bash
    Steps:
      1. Run scylla_setup with specific flags (e.g., --nic eth0 --disks /dev/sdb)
      2. Capture output for the "non-interactive command" echo at the end
      3. Assert the printed command includes all provided flags
    Expected Result: Reproducible command printed
    Evidence: .sisyphus/evidence/task-28-setup-echo.txt
  ```
  **Commit**: YES — `feat(scripts-rs): add scylla_setup orchestrator Rust replacement`

### Wave 6 — Integration and Packaging (depends on all binaries built)

- [ ] 29. install.sh Integration — wire Rust binaries into packaging

  **What to do**:
  - Modify `install.sh` to install Rust binaries using a coexistence strategy with distinct paths:
    - **Current behavior preserved**: `cp -pr dist/common/scripts/* "$rprefix"/scripts` stays, followed by existing `relocate_python3` loop (lines 623-626) which rewrites Python scripts into `$rprefix/scripts/{name}` (bash launcher) + `$rprefix/scripts/libexec/{name}` (actual Python)
    - **Rust binaries added**: new block AFTER `relocate_python3` loop copies `target/release/{binary}` to `$rprefix/scripts/.rust/{name}`
    - **Selector wrappers**: replace the `relocate_python3`-generated bash launcher at `$rprefix/scripts/{name}` with a selector wrapper that checks `SCYLLA_USE_RUST=1` env var or `/etc/scylla.d/use-rust-scripts` flag file; if set → exec `.rust/{name} "$@"`, else → exec existing relocated Python via `libexec/{name}` (preserving PYTHONPATH, PATH, SSL_CERT_FILE env setup from original relocate_python3 launcher)
    - **Layout after install**: `$rprefix/scripts/{name}` = selector wrapper, `$rprefix/scripts/.rust/{name}` = Rust binary, `$rprefix/scripts/libexec/{name}` = Python original (from existing relocate_python3)
    - Default behavior (no env var/flag file): wrapper calls `libexec/{name}` (Python) → zero behavioral change on upgrade
  - Key areas in install.sh to modify:
    - Line 501-502: `cp -pr dist/common/scripts/* "$rprefix"/scripts` — keep as-is
    - Lines 623-626: `relocate_python3` loop — keep as-is, generates `libexec/{name}` + bash launcher
    - NEW block after line 626: for each Rust binary, copy to `.rust/` and replace the launcher at `$rprefix/scripts/{name}` with selector wrapper
    - Line 578-581: `scylla_sysconfdir.py` generation — also generate `scylla_sysconfdir.conf` with `SYSCONFDIR="$sysconfdir"` (matching the Python file's variable name)
  - Add `cargo build --release` invocation (or document as prerequisite)

  **Must NOT do**: Remove Python scripts (incremental rollout), modify systemd unit files, change `$rprefix/scripts` base path, break `relocate_python3` flow

  **Recommended Agent Profile**: **Category**: `unspecified-high`, **Skills**: []

  **Parallelization**: Wave 6. **Blocked By**: T28 (all binaries must build). **Blocks**: F1-F4.

  **References**:
  - `install.sh` line 501-502 — `cp -pr dist/common/scripts/* "$rprefix"/scripts`
  - `install.sh` line 514 — `SBINFILES=$(cd dist/common/scripts/; ls scylla_*setup node_health_check scylla_kernel_check)`
  - `install.sh` lines 578-581 — `scylla_sysconfdir.py` generation
  - `install.sh` lines 79-83 — `--root` and `--prefix` flags (NOT DESTDIR)
  - Cargo workspace at `dist/common/scripts-rs/Cargo.toml` (T1)

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: install.sh installs selector wrapper + Rust + relocated Python at correct paths
    Tool: Bash
    Steps:
      1. Build all Rust binaries: cargo build --release (in scripts-rs/)
      2. Run: ./install.sh --root /tmp/scylla-install --prefix /opt/scylladb --nonroot
      3. Assert selector wrapper at /tmp/scylla-install/opt/scylladb/scripts/scylla_stop (shell script containing "SCYLLA_USE_RUST")
      4. Assert Rust binary at /tmp/scylla-install/opt/scylladb/scripts/.rust/scylla_stop (ELF)
      5. Assert relocated Python at /tmp/scylla-install/opt/scylladb/scripts/libexec/scylla_stop (from relocate_python3)
      6. Assert all are executable
    Expected Result: Three artifacts per script at distinct paths
    Evidence: .sisyphus/evidence/task-29-install-layout.txt

  Scenario: Wrapper dispatches to Python by default, Rust when opted in
    Tool: Bash
    Steps:
      1. Run: /tmp/scylla-install/opt/scylladb/scripts/scylla_dev_mode_setup --help (no env var)
      2. Assert output does NOT include --verbose (Python via libexec/ was called)
      3. SCYLLA_USE_RUST=1 /tmp/scylla-install/opt/scylladb/scripts/scylla_dev_mode_setup --help
      4. Assert output includes --verbose (Rust binary via .rust/ was called)
    Expected Result: Default=Python, SCYLLA_USE_RUST=1=Rust
    Evidence: .sisyphus/evidence/task-29-install-selector.txt

  Scenario: scylla_sysconfdir.conf generated with correct variable name
    Tool: Bash
    Steps:
      1. After install, check /tmp/scylla-install/opt/scylladb/scripts/scylla_sysconfdir.conf exists
      2. Assert contains SYSCONFDIR= (matching scylla_sysconfdir.py's variable name)
      3. Assert value matches scylla_sysconfdir.py content
    Expected Result: Config file present with SYSCONFDIR= and correct path
    Evidence: .sisyphus/evidence/task-29-install-sysconfdir.txt
  ```
  **Commit**: YES — `feat(scripts-rs): integrate Rust binaries into install.sh`

- [ ] 30. Documentation — migration notes and debug guide

  **What to do**:
  - Add `dist/common/scripts-rs/README.md`:
    - Architecture overview (Cargo workspace, shared lib, bin crates)
    - How to build: `cargo build --release`
    - How to test: `cargo test`
    - How to add a new script
    - Debug features: `--verbose`, `--dry-run`, `--diagnose`, crash context files
  - Add inline doc comments to shared lib crate (lib.rs, key public functions)
  - Add `MIGRATION.md` in scripts-rs/:
    - Per-script migration status checklist
    - Known behavioral differences (if any, e.g., the ntp_setup bug fix)
    - How to switch between Python and Rust versions
    - Rollback instructions
  - Add DebugArgs: N/A (documentation task)

  **Must NOT do**: Add user-facing documentation outside the scripts-rs directory, modify existing ScyllaDB docs

  **Recommended Agent Profile**: **Category**: `writing`, **Skills**: []

  **Parallelization**: Wave 6. **Blocked By**: T28 (needs complete picture). **Blocks**: F1-F4.

  **References**:
  - All bin crates T6-T28 — for per-script status
  - Shared lib crate T2-T5 — for architecture description
  - `dist/common/scripts-rs/Cargo.toml` — workspace definition
  - DebugArgs pattern from T3 — document all debug flags

  **QA Scenarios (MANDATORY):**
  ```
  Scenario: README.md build instructions work
    Tool: Bash
    Steps:
      1. Follow README.md instructions to build from scratch
      2. cargo build --release
      3. cargo test
      4. Assert both succeed
    Expected Result: Documentation-driven build succeeds
    Evidence: .sisyphus/evidence/task-30-readme-build.txt

  Scenario: MIGRATION.md covers all scripts
    Tool: Bash
    Steps:
      1. Count scripts in dist/common/scripts/ (excluding node_health_check, cqlsh, perftune.py)
      2. Count entries in MIGRATION.md checklist
      3. Assert counts match
    Expected Result: Every in-scope script has a migration entry
    Evidence: .sisyphus/evidence/task-30-migration-coverage.txt
  ```
  **Commit**: YES — `docs(scripts-rs): add README and migration guide`

---

## Final Verification Wave (MANDATORY — after ALL implementation tasks)

> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.

- [ ] F1. **Plan Compliance Audit** — `oracle`
  Read the plan end-to-end. For each "Must Have": verify implementation exists (read file, run binary). For each "Must NOT Have": search codebase for forbidden patterns — reject with file:line if found. Check evidence files exist in .sisyphus/evidence/. Compare deliverables against plan.
  Output: `Must Have [N/N] | Must NOT Have [N/N] | Tasks [N/N] | VERDICT: APPROVE/REJECT`

- [ ] F2. **Code Quality Review** — `unspecified-high`
  Run `cargo clippy`, `cargo test`, `cargo fmt --check`. Review all crates for: `unwrap()` in non-test code, `unsafe` blocks, hardcoded paths that should be configurable, missing error context. Check that every binary has verbose/dry-run/diagnose support.
  Output: `Build [PASS/FAIL] | Clippy [PASS/FAIL] | Tests [N pass/N fail] | VERDICT`

- [ ] F3. **Real Manual QA** — `unspecified-high`
  For each migrated binary: for scripts with argparse, run with `--help` and diff against Python original's `--help` (filtering debug flags). For scripts without argparse (see QA Policy exception list), verify exit-code and output parity instead. Test on a mock environment with scylla dirs. Verify crash context dump works (trigger a panic path). Test verbose mode output. Save to `.sisyphus/evidence/final-qa/`.
  Output: `Binaries [N/N parity-verified] | Crash dumps [N/N] | Verbose [N/N] | VERDICT`

- [ ] F4. **Scope Fidelity Check** — `deep`
  For each task: read "What to do", read actual code. Verify 1:1 — everything in Python was built (no missing behavior), nothing beyond Python was built (no creep). Check "Must NOT do" compliance. Flag any Rust binary that adds features not in Python original. **Exemption**: `--verbose`, `--dry-run`, `--diagnose` flags and `SCYLLA_SCRIPTS_TEST_*` env var overrides are plan-mandated additions and must NOT be flagged as feature creep.
  Output: `Tasks [N/N compliant] | Feature creep [CLEAN/N issues] | VERDICT`

---

## Commit Strategy

| Wave | Commit | Message |
|------|--------|---------|
| 1 | T1-T5 together | `feat(scripts-rs): scaffold Cargo workspace with shared lib crate` |
| 2 | Each script individually or grouped | `feat(scripts-rs): add scylla_stop Rust replacement` |
| 3 | Each script individually or grouped | `feat(scripts-rs): add scylla_ntp_setup Rust replacement` |
| 4 | Each script individually | `feat(scripts-rs): add scylla_raid_setup Rust replacement` |
| 5 | T28 alone | `feat(scripts-rs): add scylla_setup orchestrator Rust replacement` |
| 6 | T29-T30 together | `feat(scripts-rs): integrate Rust binaries into install.sh` |

---

## Success Criteria

### Verification Commands
```bash
cargo build --release              # Expected: all binaries compile
cargo test                         # Expected: all tests pass
cargo clippy -- -D warnings        # Expected: no warnings
# --help parity check (only for scripts that have argparse in Python original)
for bin in scylla_dev_mode_setup scylla_rsyslog_setup \
           scylla_config_get.py scylla-housekeeping scylla_memory_setup scylla_cpuset_setup \
           scylla_nofile_setup scylla_cpuscaling_setup scylla_ntp_setup scylla_sysconfig_setup \
           scylla_swap_setup scylla_coredump_setup scylla_io_setup scylla_raid_setup \
           scylla_setup scylla-blocktune; do
  diff <(python3 "dist/common/scripts/$bin" --help 2>&1) \
       <(./target/release/"$bin" --help 2>&1 | grep -v -E '^\s*--(verbose|dry-run|diagnose)')
done                               # Expected: zero diff per binary (debug flags excluded)
# Scripts without --help (scylla_logrotate, scylla_fstrim_setup, scylla_kernel_check, scylla_fstrim,
# scylla_stop, scylla_prepare, scylla_selinux_setup):
# verified via exit-code and output-format parity in their individual QA scenarios
```

### Final Checklist
- [ ] All "Must Have" present
- [ ] All "Must NOT Have" absent
- [ ] All cargo tests pass
- [ ] All binaries produce identical --help to Python originals (except added --verbose/--dry-run/--diagnose)
- [ ] All binaries support --verbose, --dry-run, --diagnose where applicable
- [ ] Crash context files work (tested via intentional error triggers)
- [ ] Tested on debian-variant and redhat-variant
