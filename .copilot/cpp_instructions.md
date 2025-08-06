This document provides specific coding standards and best practices for C++ development within this repository. Adhering to these rules ensures code consistency, maintainability, and high performance.

# 1. Coding Style and Formatting

* **Auto-Formatting:** All C++ code must be formatted using the `.clang-format` file located in the repository root. This is a non-negotiable rule.
* **Line Length:** The preferred line length is 120 characters, with a hard maximum of 160 characters for complex statements or function signatures to maintain readability.
* **Brace Style:** Opening braces (`{`) should be on the same line as the control structure or function definition.
* **Indentation:** Use 4 spaces for indentation.

# 2. Naming Conventions

* **`snake_case`:** All identifiers, including classes, structs, enums, functions, variables, member variables, and namespaces, must be in `snake_case`.
    * **Example:** `my_class_name`, `http_request`, `local_variable`.
* **Constants:** Global constants and `constexpr` variables should also be in `snake_case`. While `UPPER_SNAKE_CASE` may be present in older code, `snake_case` is the preferred convention for new code.
    * **Example:** `static constexpr int max_connections = 100;`
* **File Naming:** Header files should have a `.hh` extension and source files a `.cc` extension. Header file names should match the main class or module they contain (e.g., `http_server.hh` for `class http_server`).

#### 3\. Modern C++ Usage

* **C++ Standard:** The codebase uses C++20 or newer. Prefer features from these standards, especially coroutines, as they improve code clarity and safety.
* **Resource Management:** Use Seastar-specific smart pointers and resource management types.
    * Prefer `foreign_ptr` for sharing resources between Seastar and other code.
    * Use `lw_shared_ptr` and `shared_ptr` for shared ownership within the Seastar framework.
    * Avoid `std::unique_ptr`, `std::shared_ptr`, and raw pointers unless there is a clear, documented reason (e.g., interfacing with C-style APIs).
* **Headers:**
    * Use `#pragma once` for include guards.
    * Include project headers first, followed by third-party library headers, and finally standard library headers.
* **No `using namespace std;`:** Never include `using namespace std;` in header files. In source files, use it sparingly and only within function scope to minimize naming conflicts.
* **Avoid Macros:** Avoid preprocessor macros as much as possible. Use `inline` functions, `constexpr` variables, or templates instead.
* **Type Safety:** Prefer scoped enums (`enum class`) over unscoped enums.
* **`const` Correctness:** Mark all functions and variables as `const` whenever their state does not change. Use `const` references for function parameters to avoid unnecessary copies.

# 4. Concurrency and Asynchrony

* **Coroutines are Preferred:** The primary asynchronous programming model is coroutines. Prefer using coroutines over traditional callbacks or continuations for all new code.
* **Seastar Framework:** Adhere to the concurrency model and best practices of the Seastar framework. The code should be written to take advantage of Seastar's shard-per-core architecture.
* **Thread Safety:** Pay close attention to thread safety. Shared data should be protected by appropriate synchronization primitives (e.g., `seastar::mutex`, `std::atomic`), or structured to avoid sharing entirely (e.g., using message passing or per-core data).
* **`co_await` Semantics:** When using `co_await`, ensure you understand the execution context and potential for context switching.
* **Avoid Blocking:** Never perform blocking I/O or long-running computations on the reactor thread. Such operations must be offloaded to dedicated thread pools.

# 5. Performance and Optimization

* **Zero-Overhead Abstractions:** Prefer abstractions that have zero or minimal runtime overhead.
* **Avoid Dynamic Allocation:** Minimize dynamic memory allocations, especially in hot code paths. Prefer stack-based or pre-allocated objects.
* **Compiler Optimizations:** Write code that is easy for the compiler to optimize. Avoid aliasing, use `constexpr` where possible, and prefer standard library algorithms that can be vectorized.
* **Benchmarking:** Any performance-critical change should be accompanied by benchmarks to validate its impact.

# 6. Testing

* **Test-Driven Development (TDD)**: When modifying existing functionality, if there is a corresponding test file, follow a TDD approach. This means first creating a new test case that reproduces the bug or verifies the new feature, then making the minimum amount of changes in the implementation to pass the test.
* **Test File Naming**: C++ unit tests often match the name of the source file they are testing (e.g., my_module_test.cc for my_module.cc).
* **Unit Tests (C++):** When feasible, generate a native C++ unit test. For asynchronous code, use the Seastar testing macros like `SEASTAR_TEST_CASE` and `SEASTAR_THREAD_TEST_CASE`. A test case should be self-contained and test a single unit of functionality.
    * **Example of an asynchronous Seastar test:**
      ```cpp
      #include <seastar/testing/test_case.hh>
      #include <seastar/core/future.hh>
      #include "path/to/my_header.hh"

      SEASTAR_TEST_CASE(my_async_test_case) {
          // Arrange
          my_class_name my_instance;

          // Act & Assert
          return my_instance.my_async_method().then([](int result) {
              BOOST_CHECK_EQUAL(result, 10);
          });
      }
      ```
* **Integration Tests (Python):** If a native C++ unit test cannot be easily created, offer a Python integration test. This test should operate at a higher level, interacting with the system as a black box (e.g., through a command-line interface or a network protocol).
    * The Python test should be in the format currently used in the codebase, likely using `pytest` or a custom framework. The test should start the C++ executable, interact with it, and assert the correct behavior.

# 7. Verification

* **Build Code After Changes:** After making a change, always perform a local build to ensure the code compiles without errors. The build system uses `ninja`.
    * **Example command:** `ninja dev-build`
* **Run Relevant Tests:** Following a successful build, run the corresponding unit or integration tests to verify the change's correctness and prevent regressions.
    * Due to the complexity of the codebase, determining the exact tests to run can be challenging. A good heuristic is to run tests from the same module as the modified code.
    * **Example commands for running tests:**
        * `./test.py --no-gather-metric --mode debug cluster/test_topology_ops` (for a specific Python integration test)
        * `ninja run-tests` (for a general set of C++ unit tests)
    * If a new test has been created, ensure that it is included in the test run.

# 8. ScyllaDB-Specific Patterns and Conventions

## Memory Management
* **Seastar Allocators:** Prefer Seastar's memory management over standard allocators in hot paths
* **lw_shared_ptr:** Use for lightweight shared ownership within Seastar contexts
* **foreign_ptr:** Use when sharing data between Seastar and external code
* **Avoid std::shared_ptr:** Unless interfacing with external C++ APIs

## Database-Specific Patterns
* **Schema Objects:** Always use schema pointers (`schema_ptr`) for schema references
* **Mutation Handling:** Use `mutation` and `mutation_partition` for data modifications
* **Key Handling:** Use appropriate key types (`partition_key`, `clustering_key`)
* **Time Handling:** Use `api::timestamp_type` for database timestamps, `gc_clock` for garbage collection timing

## Asynchronous Programming Patterns
* **Coroutines First:** Prefer `co_await` over `.then()` chains for readability
* **Seastar Futures:** Always use `seastar::future<T>` for async operations
* **Gate Management:** Use `seastar::gate` for proper shutdown coordination
* **Semaphore Usage:** Use `seastar::semaphore` for resource limiting, not `std::mutex`

## Performance-Critical Guidelines
* **Avoid Dynamic Allocation:** In hot paths, prefer stack allocation or pre-allocated pools
* **NUMA Awareness:** Consider shard-per-core architecture in design
* **Batch Operations:** Group operations to reduce syscall overhead
* **Lock-Free Programming:** Prefer message passing over shared mutable state

# 9. Code Quality and Maintenance

* **Testability:** Write code that is easy to test. Use dependency injection to isolate components where necessary.
* **Rule of Zero/Three/Five:** Classes that manage resources should explicitly define their move/copy constructors and assignment operators or explicitly delete them, following the Rule of Zero/Three/Five.
* **Error Handling:** Use `seastar::future<T>` to propagate errors in asynchronous code. Avoid using exceptions for control flow in performance-sensitive paths.
* **Clarity over Abstraction:** While abstraction is good, avoid premature or overly complex abstractions. Code should be as simple and direct as possible while achieving its goals.
* **Documentation:** Include meaningful comments for complex algorithms, performance optimizations, or non-obvious design decisions. Avoid commenting obvious code.
