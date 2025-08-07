This file serves as a high-level guide for all code written in this repository. It establishes a common understanding of our project's goals and values. For more detailed guidance, see:

* **`cpp_instructions.md`** - C++ specific coding standards and patterns
* **`python_instructions.md`** - Python testing guidelines and conventions
* **`testing_instructions.md`** - Comprehensive testing framework guidance
* **`development_workflow.md`** - Project structure and development workflows
* **`troubleshooting_guide.md`** - Error handling, debugging, and common issues

# 1. Project Context

* **Primary Purpose:** This repository contains the source code for a high-performance, distributed NoSQL database.
* **Key Values:** The core values are **performance**, **correctness**, **readability**, and **maintainability**.
* **Target Audience:** The code is maintained by a large, distributed team of professional developers. Code quality is paramount to ensure the project's long-term health.

# 2. General Code Philosophy

* **Performance is a Feature:** The core code is written in C++ with an explicit focus on performance. Code should be efficient, avoid unnecessary allocations and copies, and leverage the hardware effectively.
* **Correctness over All Else:** A database must be correct. Suggestions that prioritize performance at the cost of correctness (e.g., race conditions, data corruption) must be avoided.
* **Readability is Key:** Code should be easy to read and understand. This is essential for a project with many contributors. Avoid overly clever or obscure constructs.
* **Self-Explanatory Code:** The code should be its own documentation. Copilot should aim to generate code where the intent is clear from the variable and function names. Comments should be used sparingly, only to explain *why* something is done, not *what* is being done.

# 3. Naming Conventions

* **Be Descriptive:** Names must be as self-explanatory as possible.
    * **Bad:** `int x;` (What is `x`?)
    * **Good:** `int row_count;`
* **Full Words Preferred:** Prefer full, descriptive words over abbreviations.
    * **Bad:** `std::string prc_tbl;`
    * **Good:** `std::string processed_table;`
* **Consistent Casing:** Use a consistent casing style. C++ code should follow the project's established style (`snake_case` for variables). Python test code should follow PEP 8 (`snake_case`).
* **Unit and Purpose:** Naming should reflect the unit of measurement or the purpose of the variable.
    * **Bad:** `auto timeout = 5;` (Is this 5 seconds? Milliseconds? What is it for?)
    * **Good:** `std::chrono::seconds request_timeout = 5s;`

# 4. Code Structure and Style

* **Follow Established Style:** When working on existing files, adhere to the established style of that file. This overrides any personal preference.
* **Modern C++ and Python:** Favor modern language features that improve clarity and safety.
    * **C++:** C++17 or C++20 is the standard. Prefer `std::optional`, `std::variant`, and smart pointers (`std::unique_ptr`, `std::shared_ptr`) for resource management.
    * **Python:** Follow PEP 8 and use modern Python features (e.g., f-strings, type hints).
* **Avoid Code Duplication:** Copilot should strive to avoid generating redundant code. If a common pattern exists, it should suggest calling or refactoring to a shared utility function.

# 5. Documentation and Comments

* **Docstrings:** Public APIs (classes, functions) in C++ headers and Python modules should be documented with a brief explanation of their purpose, parameters, and return values.
* **In-Code Comments:** Comments should be used as a last resort. If a comment is needed to explain a piece of code, it may be a sign that the code is too complex and should be simplified or refactored.
    * **Good use of comments:** Explaining a non-obvious algorithm, a performance optimization, or a workaround for a specific bug.
    * **Bad use of comments:** "Increment the counter" (`counter++;`).

# 6. Logging

* **Be Mindful of Noise:** Logs should provide meaningful information without overwhelming the system or developers. The goal is to produce logs that are useful for debugging, not just a stream of every event.
* **Use Appropriate Log Levels:**
    * **`error`:** For unrecoverable failures that require immediate attention (e.g., a node crash, a critical resource failure).
    * **`warn`:** For unexpected but recoverable events or potential issues that may become a problem later (e.g., high latency, a failed retry).
    * **`info`:** For significant events in the application's lifecycle, like starting, stopping, or completing major operations. This is the default level for production.
    * **`debug` and `trace`:** These levels can be more verbose and are intended for developers to diagnose specific issues. They should not be used for production unless actively troubleshooting a problem.
* **Context is Key:** When logging, include enough context to diagnose the issue. This means providing relevant variables, unique identifiers (e.g., `request_id`, `session_id`), timestamps, and the specific module or function where the log was generated.
* **No Sensitive Data:** Never log sensitive information such as user credentials, personally identifiable information (PII), or private keys.
* **Structured Logging:** Prefer structured log messages (e.g., JSON-like key-value pairs) where possible, as they are easier to parse, search, and analyze with external tools.

# 7. Building and Testing

## Build Configuration and Modes

* **Build Modes:** The project supports three build modes:
    * **`dev`:** Default mode for local development and testing. Contains debug information but optimized for development workflow.
    * **`debug`:** Used for debugging and catching issues (e.g., AddressSanitizer failures). More verbose debugging information.
    * **`release`:** Production optimized build. Rarely used in local testing as `dev` mode is sufficient.
* **Configuration Required:** Before building, configure the project for your target mode: `./configure.py --disable-dpdk --use-profile= --mode=dev`
* **Configuration is Persistent:** Only needs to be done once per mode, or when switching between modes. Always prefer `dev` mode for local development.

## Building the Code

* **Full Build:** Use `ninja dev-build` (or `ninja -j150 dev-build` on powerful build machines). This builds Scylla binary and all tests.
* **Scylla Binary Only:** For Python tests, only the main binary is needed: `ninja build/dev/scylla`
* **Specific C++ Tests:** Build individual test binaries: `ninja build/dev/test/boost/<test_name>`
* **Combined Tests:** Many C++ tests are in a composite binary: `ninja build/dev/test/boost/combined_tests`
* **Job Count:** On local laptops, omit `-j` flag for optimal performance. Use `-j150` only on distributed build machines.

## Running Tests

### C++ Unit Tests
* **Combined Tests:** `./build/dev/test/boost/combined_tests --run_test=<test_suite_name>`
* **Individual Tests:** `./build/dev/test/boost/<test_binary>`
* **Via test.py:** `./test.py --no-gather-metric --mode dev boost/<test_name>`

### Python Integration Tests
* **Prerequisites:** Requires Scylla binary: `ninja build/dev/scylla`
* **Command:** `./test.py --no-gather-metric --mode dev <test_path>`
* **Example:** `./test.py --no-gather-metric --mode dev cluster/test_raft_no_quorum`
* **Verbose Output:** Add `-v` flag for debugging test failures
* **Logs:** Test logs are in `testlog/dev/`, failed tests in `testlog/dev/failed_tests/`

## Post-Change Workflow

* **Always Suggest Testing:** After making code changes, Copilot should suggest:
    1. **Build:** Appropriate build command based on the change (full build, Scylla binary, or specific test)
    2. **Test:** Relevant tests to verify the change works correctly
    3. **For C++ changes:** Suggest building and running related unit tests
    4. **For Python test changes:** Suggest building Scylla binary and running the modified tests
    5. **For core functionality changes:** Suggest running both unit and integration tests

## Build and Test Examples

```bash
# Configuration (once per mode)
./configure.py --disable-dpdk --use-profile= --mode=dev

# Full build
ninja dev-build

# Build for Python tests only
ninja build/dev/scylla

# Build and run specific C++ test
ninja build/dev/test/boost/combined_tests
./build/dev/test/boost/combined_tests --run_test=group0_voter_calculator_test

# Run Python integration test
./test.py --no-gather-metric --mode dev test_raft_no_quorum

# Run test with verbose output for debugging
./test.py --no-gather-metric --mode dev cluster/test_tablets_lwt -v
```

# 8. Debugging and Troubleshooting

## Common Issues and Solutions
* **Build Failures:** Check dependencies and run `./configure.py` with appropriate flags
* **Test Failures:** Check `testlog/dev/` for detailed logs and error messages

## Debug Build Configuration
* **Debug Symbols:** Use `--tests-debuginfo` flag when building tests for debugging
* **Sanitizers:** Use `sanitize` mode to catch memory errors and undefined behavior
* **Verbose Logging:** Enable trace-level logging for detailed execution flow

## IDE and Development Tool Integration
* **Compile Commands:** Use generated `compile_commands.json` for IDE integration
* **Clang-Format:** Ensure `.clang-format` is used by your IDE for consistent formatting
* **GDB Integration:** Use provided `.gdbinit` and `scylla-gdb.py` for enhanced debugging
