This document provides guidance on ScyllaDB's project structure, development workflows, and common development tasks.

# 1. Project Structure Overview

## Core Directories
* **`/`** - Root source files (main.cc, core headers)
* **`api/`** - External API implementations (CQL, REST)
* **`alternator/`** - DynamoDB-compatible API implementation
* **`auth/`** - Authentication and authorization
* **`cdc/`** - Change Data Capture functionality
* **`cql3/`** - CQL (Cassandra Query Language) implementation
* **`db/`** - Core database functionality
* **`dht/`** - Distributed Hash Table implementation
* **`gms/`** - Gossip and membership service
* **`locator/`** - Token ring and replica placement
* **`mutation/`** - Data mutation and versioning
* **`raft/`** - Raft consensus algorithm implementation
* **`repair/`** - Data repair and anti-entropy
* **`sstables/`** - SSTable (Sorted String Table) implementation
* **`streaming/`** - Data streaming for repairs and bootstrap
* **`utils/`** - Utility functions and data structures

## Test Directories
* **`test/boost/`** - C++ unit tests using Boost.Test
* **`test/unit/`** - Additional unit tests
* **`test/raft/`** - Raft-specific tests
* **`test/cqlpy/`** - Python CQL integration tests
* **`test/alternator/`** - DynamoDB API tests
* **`test/topology*/`** - Multi-node cluster tests
* **`test/perf/`** - Performance benchmarks
* **`test/manual/`** - Manual testing utilities

## Build and Configuration
* **`cmake/`** - CMake build system files
* **`conf/`** - Default configuration files
* **`tools/`** - Development and debugging tools
* **`seastar/`** - Seastar framework (submodule)
* **`abseil/`** - Abseil C++ library (submodule)

# 2. Development Workflow

## Setting Up Development Environment
1. **Initial Setup:** Follow installation instructions for dependencies
2. **Configuration:** Run `./configure.py --disable-dpdk --mode=dev`
3. **Build:** Use `ninja dev-build` for full build or `ninja build/dev/scylla` for binary only
4. **IDE Setup:** Configure your IDE to use the generated `compile_commands.json`

## Common Development Tasks

### Building Specific Components
* **Full build:** `ninja dev-build`
* **Main binary:** `ninja build/dev/scylla`
* **Specific test:** `ninja build/dev/test/boost/test_name`
* **All tests:** `ninja build/dev/test/boost/combined_tests`

### Running Tests
* **Single unit test:** `./test.py --mode dev boost/test_name.cc`
* **Test with specific case:** `./test.py --mode dev boost/test_name.cc::case_name`
* **Python test:** `./test.py --mode dev cqlpy/test_file.py`
* **All boost tests:** `./test.py --mode dev boost/`

### Debugging
* **GDB with Scylla:** Use provided `.gdbinit` and `scylla-gdb.py`
* **Test debugging:** Build with `--tests-debuginfo` for debug symbols
* **Log analysis:** Check `testlog/dev/` for test outputs

## Code Organization Principles

### File Organization
* **Header files:** Use `.hh` extension, place in same directory as implementation
* **Source files:** Use `.cc` extension
* **Template implementations:** Keep in header files or use `.hh` extension
* **Forward declarations:** Use separate `_fwd.hh` files when needed

### Module Dependencies
* **Core modules:** Should have minimal dependencies
* **API modules:** Can depend on core but not on each other
* **Test modules:** Can depend on anything needed for testing
* **Utilities:** Should be standalone with minimal dependencies

# 3. Development Best Practices

## Code Changes Workflow
1. **Identify scope:** Determine which modules are affected
2. **Write tests first:** Add tests for new functionality or reproduce bugs
3. **Implement changes:** Make minimal changes to achieve the goal
4. **Build and test:** Ensure all relevant tests pass
5. **Performance check:** Run relevant performance tests if applicable

## Working with Existing Code
* **Follow existing patterns:** Match the style and patterns of surrounding code
* **Understand dependencies:** Check what depends on your changes using build system
* **Test existing functionality:** Ensure your changes don't break existing features
* **Documentation:** Update comments and documentation for significant changes

## Adding New Features
* **Design discussion:** Consider the design implications and discuss with team
* **Interface design:** Design clean, minimal interfaces
* **Test coverage:** Ensure comprehensive test coverage including edge cases
* **Performance impact:** Consider and measure performance implications
* **Documentation:** Add or update relevant documentation

# 4. Build System Understanding

## Ninja Build System
* **Generated from:** `configure.py` creates `build.ninja`
* **Parallel builds:** Use `-j` flag appropriately for your machine
* **Incremental builds:** Ninja automatically handles dependencies
* **Clean builds:** Remove `build/` directory or use mode-specific directories

## Configuration Options
* **Build modes:** `dev`, `debug`, `release`, `sanitize`
* **Architecture options:** Various CPU and platform-specific optimizations
* **Feature flags:** Enable/disable specific features during build
* **Debug options:** Control debug information and optimizations

## Dependencies Management
* **Submodules:** Use `git submodule sync && git submodule update --init --force --recursive`
* **System dependencies:** Managed through package managers
* **External libraries:** Some bundled, others system-provided

# 5. Debugging and Profiling

## Debugging Tools
* **GDB:** Enhanced with Scylla-specific pretty printers
* **AddressSanitizer:** Use `sanitize` build mode
* **Valgrind:** Supported with special configuration
* **Core dumps:** Automatic collection and analysis tools

## Performance Analysis
* **Built-in metrics:** Extensive metrics collection and reporting
* **Profiling:** CPU and memory profiling tools
* **Benchmarking:** Performance test suite in `test/perf/`
* **Tracing:** Distributed tracing support

## Log Analysis
* **Structured logging:** JSON and key-value logging
* **Log levels:** Appropriate use of trace, debug, info, warn, error
* **Log rotation:** Automatic log rotation and cleanup
* **Debugging logs:** Verbose logging for troubleshooting

# 6. Common Patterns and Idioms

## Error Handling Patterns
* **Future-based errors:** Use `seastar::future<T>` for async error propagation
* **Exception handling:** Appropriate use of exceptions vs. error codes
* **Validation:** Input validation and sanitization patterns
* **Resource cleanup:** RAII and scope-based cleanup

## Asynchronous Programming Patterns
* **Coroutines:** Modern C++20 coroutine patterns
* **Future composition:** Chaining and combining async operations
* **Parallelism:** Running operations in parallel safely
* **Synchronization:** Coordinating between async operations

## Data Structure Patterns
* **Immutable data:** Prefer immutable data structures where possible
* **Copy-on-write:** Efficient sharing of large data structures
* **Memory pools:** Custom allocators for performance-critical paths
* **Lock-free structures:** When appropriate for high-performance scenarios

# 7. Integration Points

## Database Integration
* **Schema management:** How to work with database schemas
* **Query processing:** Integration with CQL query processor
* **Storage engine:** Interaction with storage layer
* **Replication:** Understanding distributed aspects

## External APIs
* **CQL protocol:** Cassandra-compatible query language
* **REST API:** HTTP-based management interface
* **DynamoDB API:** Amazon DynamoDB-compatible interface
* **Monitoring:** Metrics and health check endpoints

## System Integration
* **Operating system:** Linux-specific optimizations and features
* **Network stack:** Seastar networking integration
* **File system:** Efficient file I/O patterns
* **Memory management:** NUMA-aware memory allocation

# 8. Release and Deployment

## Code Quality Gates
* **Static analysis:** Automated code quality checks
* **Test coverage:** Comprehensive test suite execution
* **Performance regression:** Performance benchmark validation
* **Documentation:** Ensuring documentation is up-to-date

## Packaging and Distribution
* **Binary packages:** RPM, DEB package creation
* **Container images:** Docker image generation
* **Configuration management:** Default configurations and customization
* **Upgrade procedures:** Database upgrade and migration procedures
