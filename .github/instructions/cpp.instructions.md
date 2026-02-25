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

## Coroutines
```cpp
seastar::future<T> func() {
    auto result = co_await async_operation();
    co_return result;
}
```

## Error Handling
- Throw exceptions for errors (futures propagate them automatically)
- In data path: avoid exceptions, use `std::expected` (or `boost::outcome`) instead
- Use standard exceptions (`std::runtime_error`, `std::invalid_argument`)
- Database-specific: throw appropriate schema/query exceptions

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
- Never `using namespace` in headers (exception: `using namespace seastar` is globally available via `seastarx.hh`)

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
- `using namespace` in headers
- Blocking operations: `std::sleep`, `std::read`, `std::mutex` (use Seastar equivalents)
- `std::atomic` (reserved for very special circumstances only)
- Macros (use `inline`, `constexpr`, or templates instead)

## Testing
When modifying existing code, follow TDD: create/update test first, then implement.
- Examine existing tests for style and structure
- Use Boost.Test framework
- Use `SEASTAR_THREAD_TEST_CASE` for Seastar asynchronous tests
- Aim for high code coverage, especially for new features and bug fixes
- Maintain bisectability: all tests must pass in every commit. Mark failing tests with `BOOST_FAIL()` or similar, then fix in subsequent commit
