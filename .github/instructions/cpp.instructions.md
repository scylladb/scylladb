---
applyTo: "**/*.{cc,hh}"
---

# C++ Guidelines

**Important:** Always match the style and conventions of existing code in the file and directory.

## Memory Management
- Prefer stack allocation whenever possible
- Use `std::unique_ptr` by default for dynamic allocations
- `new`/`delete` are forbidden (use RAII)
- Use `seastar::lw_shared_ptr` or `seastar::shared_ptr` for shared ownership within same shard
- Use `seastar::foreign_ptr` for cross-shard sharing
- Avoid `std::shared_ptr` except when interfacing with external C++ APIs
- Avoid raw pointers except for non-owning references or C API interop

## Seastar Asynchronous Programming
- Use `seastar::future<T>` for all async operations
- Prefer coroutines (`co_await`, `co_return`) over `.then()` chains for readability
- Coroutines are preferred over `seastar::do_with()` for managing temporary state
- In hot paths where futures are ready, continuations may be more efficient than coroutines
- Chain futures with `.then()`, don't block with `.get()` (unless in `seastar::thread` context)
- All I/O must be asynchronous (no blocking calls)
- Use `seastar::gate` for shutdown coordination
- Use `seastar::semaphore` for resource limiting (not `std::mutex`)
- Break long loops with `maybe_yield()` to avoid reactor stalls
- Most Scylla code runs on a single shard where atomics are unnecessary
- Use Seastar message passing for cross-shard communication

## Coroutines
```cpp
seastar::future<T> func() {
    auto result = co_await async_operation();
    co_return result;
}
```

## Lifetimes
- In a function that returns the future of a `.then()` chain, capture locals by value or move, and
  capture `this` only when the object outlives the continuation: a captured reference to a local
  dangles once the function returns. A coroutine that awaits the chain can capture its own locals by
  reference, since they live in the coroutine frame
- A coroutine parameter taken by value lives in the coroutine frame and stays valid across
  `co_await`. An lvalue reference parameter stays valid only while the caller keeps the object alive
  and at the same address. Use an rvalue reference parameter only before the first `co_await`, or
  move it into a local first. Take parameters by value when in doubt
- The captures of a lambda coroutine live in the lambda object, not in the coroutine frame: they
  dangle after the first suspension unless the lambda object stays alive until the coroutine
  completes
- After a `co_await`, look up iterators, pointers and references into shared state (a container, the
  schema) again: another fiber can invalidate them during the suspension

## Error Handling
- Throw exceptions for errors (futures propagate them automatically)
- In coroutines, use `co_await coroutine::return_exception_ptr()` or `co_return coroutine::exception()` to avoid the overhead of throwing
- To pass on or handle the failure of an awaited future without rethrowing it, use
  `co_await coroutine::try_future(f)`, which returns the exception to the caller, or
  `co_await coroutine::as_future(f)`, which gives the future to check with `failed()`.
  A plain `co_await f` rethrows the exception inside the coroutine
- In data path: avoid exceptions, use `std::expected` (or `boost::outcome`) instead
- Use standard exceptions (`std::runtime_error`, `std::invalid_argument`)
- Database-specific: throw appropriate schema/query exceptions
- Report errors meant for a CQL client with a subclass of `exceptions::cassandra_exception`: any
  other exception reaches the client as SERVER_ERROR. For a user mistake, use the type the code
  already uses for similar mistakes, e.g. `exceptions::invalid_request_exception`

## Invariant Checking
- Prefer `throwing_assert()` (`utils/assert.hh`), it logs and throws instead of aborting
- Use `SCYLLA_ASSERT` where critical to system stability where no clean shutdown is possible, it aborts
- Use `on_internal_error()` for should-never-happen conditions that should be logged with backtrace

## Performance
- Pass large objects by `const&` or `&&` (move semantics)
- Use `std::string_view` for non-owning string references
- Avoid copies: prefer move semantics
- Use `utils::chunked_vector` instead of `std::vector` for large allocations (>128KB)
- Minimize dynamic allocations in hot paths

## Database-Specific Types
- Use `schema_ptr` for schema references
- Use `mutation` and `mutation_partition` for data modifications
- Use `partition_key` and `clustering_key` for keys
- Use `api::timestamp_type` for database timestamps
- Use `gc_clock` for garbage collection timing
- Use `utils::tagged_uuid` (see `utils/UUID.hh`) for UUID-based identifiers instead of a bare
  `utils::UUID`, with a tag type unique to each kind of id, e.g.
  `using table_id = utils::tagged_uuid<struct table_id_tag>;`. Different kinds of ids are then
  distinct types and cannot be mixed up (see also `host_id`, `tablet_task_id`, `query_id`)

## Style
- C++23 standard (prefer modern features, especially coroutines)
- Use `auto` when type is obvious from RHS
- Avoid `auto` when it obscures the type
- Use range-based for loops: `for (const auto& item : container)`
- Use standard algorithms when they clearly simplify code (e.g., replacing 10-line loops)
- Avoid chaining multiple algorithms if a straightforward loop is clearer
- Mark functions and variables `const` whenever possible
- Use scoped enums: `enum class` (not unscoped `enum`)

## Headers
- Use `#pragma once`
- Include order: own header, C++ std, Seastar, Boost, project headers
- Forward declare when possible
- Never `using namespace` in headers. Exception: most headers include `seastarx.hh`, which provides `using namespace seastar` project-wide.

## Documentation
- Public APIs require clear documentation
- Implementation details should be self-evident from code
- Use `///` or Doxygen `/** */` for public documentation, `//` for implementation notes - follow the existing style

## Naming
- `snake_case` for most identifiers (classes, functions, variables, namespaces)
- Template parameters: `CamelCase` (e.g., `template<typename ValueType>`)
- Member variables: prefix with `_` (e.g., `int _count;`)
- Structs (value-only): no `_` prefix on members
- Constants and `constexpr`: `snake_case` (e.g., `static constexpr int max_size = 100;`)
- Files: `.hh` for headers, `.cc` for source

## Formatting
- 4 spaces indentation, never tabs
- Opening braces on same line as control structure (except namespaces)
- Space after keywords: `if (`, `while (`, `return `
- Whitespace around operators matches precedence: `*a + *b` not `* a+* b`
- Line length: keep reasonable (<160 chars), use continuation lines with double indent if needed
- Brace all nested scopes, even single statements
- Minimal patches: only format code you modify, never reformat entire files

## Logging
- Use structured logging with appropriate levels: DEBUG, INFO, WARN, ERROR
- Include context in log messages (e.g., request IDs)
- Never log sensitive data (credentials, PII)

## Forbidden
- `malloc`/`free`
- `printf` family (use logging or fmt)
- Raw pointers for ownership
- Blocking operations: `std::sleep`, `std::read`, `std::mutex` (use Seastar equivalents)
- New ad-hoc macros (prefer `inline`, `constexpr`, or templates; established project macros like `SCYLLA_ASSERT` are fine)

## Testing
When modifying existing code, follow TDD: create/update test first, then implement.
- Examine existing tests for style and structure
- Use Boost.Test framework
- Use `SEASTAR_THREAD_TEST_CASE` for Seastar asynchronous tests
- Aim for high code coverage, especially for new features and bug fixes
- Maintain bisectability: all tests must pass in every commit. Mark failing tests with `BOOST_FAIL()` or similar, then fix in subsequent commit
