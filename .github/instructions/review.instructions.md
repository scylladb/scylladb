---
applyTo: "**/*.{cc,hh,py,idl.hh}"
---
 
# ScyllaDB Code Review Skill
 
You are reviewing a patch to the ScyllaDB codebase. Review it with the depth and rigor of a ScyllaDB maintainer. Your review must be precise, actionable, and grounded in the project's values: **performance, correctness, and readability**.
 
Do not produce generic or shallow feedback. Every comment must be specific to the code being reviewed and must explain **why** the change is needed. If the code looks good, say so briefly and move on.
 
## How to Perform the Review
 
1. **Read the full diff** before commenting. Understand the purpose of the change.
2. **Check the commit messages**: each commit message must explain "why", not just "what". The PR title and cover letter must be up to date with the actual changes.
3. **Review each commit individually** if the PR has multiple commits. Each commit must be self-contained, bisectable, and all tests must pass at every commit.
4. **Check for consistency**: the commit title, message, code, and tests must all describe the same thing. Flag any mismatch.
5. **Flag anything you are uncertain about** rather than staying silent. Ask questions.
 
## Critical Review Categories
 
### 1. Correctness and Safety
 
#### Assertions and Crash Safety
- **NEVER use `SCYLLA_ASSERT`, `assert()`, `std::terminate()`, `abort()`, or `on_fatal_internal_error()` for conditions that can be triggered by user input, network issues, or operational scenarios.** These kill the entire node, potentially causing cascading failures across the cluster.
  - Use `on_internal_error()` instead (crashes only in debug mode, throws in release).
  - Use exceptions for recoverable errors.
  - Use `throwing_assert()` as another option when a simple boolean check with an exception is sufficient.
- Only use hard assertions for genuinely impossible states (true invariant violations).
- When reviewing new assertions, ask: "Can this condition ever occur in production due to bad input, timing, or a bug in another component?" If yes, it must not crash the node.
 
#### Race Conditions and Concurrency
- Watch for use-after-free when lambdas capture references to objects that may be destroyed before the lambda executes.
- With coroutines: if a lambda captures `this` or references to member variables and the coroutine suspends, the object might be destroyed. Use `auto self = shared_from_this()` or capture by value.
- Check that `abort_source` and `gate` are used correctly for shutdown coordination.
- Watch for patterns where holding a `token_metadata` reference across a `co_await` can cause deadlocks (barriers wait for `tm` to die).
- Beware the lambda-coroutine fiasco: if a lambda is a coroutine and captures by reference, the captures may be destroyed before the coroutine completes. Use `[self = shared_from_this()]` or capture by value. Consider whether the lambda needs `auto this`.
- Watch for use-after-move bugs: using a value after it has been `std::move()`d is undefined behavior. Flag these actively.
- When ignoring a future, explain why it does not create a use-after-free. Never silently discard futures.
 
#### Exception Safety
- When using `coroutine::return_exception`, note that it bypasses try-catch blocks. Flag this and require a comment if intentional.
- Check that exception types match what callers expect (e.g., `mutation_write_timeout_exception` for timeouts so drivers handle retries correctly).
- Don't hardcode error message strings in both production code and tests. Error messages are not part of any contract with users and create dual-maintenance burden.
- `abort_source::subscribe` returns an empty optional if the source is already aborted. Check that callers handle this (it is a common footgun).
 
### 2. Performance
 
#### Hot Path Awareness
- In data read/write paths and inner loops, every allocation counts.
- Extracting code into a new coroutine function introduces a heap allocation for the coroutine frame. Flag this if it happens on a hot path.
- Prefer `std::string_view` over `std::string` for non-owning references.
- Use `std::move()` where values are consumed. Flag unnecessary copies.
- Prefer `reserve()` on vectors when the size is known or can be estimated; for constructing from a range, use the `std::from_range_t` constructor.
- Use `utils::chunked_vector` instead of `std::vector` for large or unbounded collections (avoids large contiguous allocations).
- Avoid `std::map` when `std::unordered_map` suffices (ordered iteration not needed).
- `reinterpret_cast` between `char*` and `uint8_t*` is legal (allowed to alias). Don't flag it.
- Use C++23 range algorithms with projections where they simplify code: `std::ranges::max(container, {}, &T::field)`.
- Prefer designated initializers over positional construction for structs.
- Avoid templates when regular function parameters work. Templates cause code bloat and should be used only when nothing else works.
- Beware of false sharing on hot counters shared between CPU cores. Align per-CPU data or sum after reducing.
 
#### Unnecessary Work
- Flag `sleep()` in tests; prefer polling with back-off or waiting for specific conditions.
- Flag redundant copies: e.g., `std::string op = rjson::to_sstring(x);` creates two copies (sstring then string).
- Flag default values that are explicitly set when they match the type's default. This adds noise without benefit.
- If a function no longer uses futures (de-futurized), update the signature to not return `future<>`.
- Flag continuation-style code (`.then()` chains) that should be rewritten as coroutines for clarity.
- When changing function parameters from rvalue-reference to by-value, require justification — this changes copy/move semantics.
 
### 3. Seastar and Async Patterns
 
- Prefer coroutines (`co_await`/`co_return`) over `.then()` chains for readability.
- `seastar::do_with()` is usually not needed with coroutines.
- Use `seastar::gate` and `gate::holder` for lifecycle management, not manual counters.
- Use `with_gate()` when the pattern matches.
- Use `coroutine::try_future()` to propagate exceptions without throwing (avoids unwinding overhead).
- Use `coroutine::return_exception()` to return exceptions without unwinding, but add a comment explaining this is intentional since it bypasses catch blocks.
- When waiting on a condition variable, check the condition in a loop (spurious wakeups).
- `maybe_yield()` in long loops to prevent reactor stalls.
- When checking `abort_source`: use `check_not_aborted()` rather than manual `if (as.abort_requested()) throw ...`.
 
### 4. Naming, Clarity, and Style
 
#### Naming
- All identifiers use `snake_case` (classes, functions, variables, namespaces). Template parameters use `CamelCase`.
- Member variables: prefix with `_`. Structs (value-only): no prefix.
- Names must be precise and descriptive:
  - "endpoint" means a node/host, not a generic thing. Don't use it for dc/rack info (use "location").
  - Function names must match behavior: a function called `get_X` should not erase data. Suggest `acquire_X` or `take_X` if it does.
  - Avoid overly generic names like `entry`, `data`, `info` without context.
- Flag abbreviations that harm readability: `evnt` (typo or abbreviation?), `desc` (descriptor? descending?).
- Don't return bare `bool` when the meaning is ambiguous at the call site. Return a small `enum class` or struct instead so callers read `result.is_ready` not just `true`.
- Template type parameters must be constrained and descriptively named. `T` alone is unacceptable when nobody can guess what it represents.
- Prefer `REQUIRE` -> something like `ENFORCE` or `CHECK` that indicates what happens on failure.
- Don't use double underscores in identifiers (`__var`); they are reserved by the C++ standard.
 
#### Type Safety and Implicit Conversions
- Constructors should be `explicit` unless implicit conversion is clearly intended and documented.
- Delete dangerous constructors (e.g., `nullptr_t`) to prevent accidental implicit conversions from `0` to `char*`.
- Conversion operators should be `explicit` unless there is a strong reason.
- Use `auto` only when the type is obvious from the RHS. When the type is simple (e.g., `bool`, `int`), write the type explicitly.
 
#### Code Clarity
- Flag "too implicit" code where the reader must mentally trace complex state to understand what happens.
- Flag communication via globals; prefer returning values and passing them explicitly.
- Flag magic numbers: e.g., a hardcoded `1000` window size should be a parameter the user of the class specifies.
- When a comment says "why" something might break in the future, require a concrete comment in the code (e.g., "add a comment to remind us if/when we have a yielding parser").
 
#### Formatting
- 4 spaces indentation, never tabs.
- Space after keywords: `if (`, `while (`, `for (`.
- Space after commas in argument lists.
- No trailing whitespace or spurious blank lines.
- Pointer star on the left side: `int* p` not `int *p`.
- Closing brace on its own line.
- **Minimal patches only**: never reformat code you didn't change. Only format the lines you modify.
- Flag unrelated whitespace changes; these must be in separate commits if intentional.
- Remove debug printouts (`print`, `std::cerr <<`) before merging. Don't add log messages redundant with existing ones nearby.
 
### 5. Headers, Includes, and File Organization
 
- **Avoid adding `#include`s to widely-used headers** (e.g., `sstables.hh`, `replica/database.hh`). These are included in hundreds of files and adding includes to them increases compile times.
- When new method signatures in popular headers require heavy dependencies (e.g., JSON libraries in `db/config.hh`), push the conversion to the caller. Return lightweight types and let callers convert.
- Prefer forward declarations over includes in header files.
- Follow include-what-you-use: include headers for what you directly use, don't rely on transitive includes.
- Include order: own header, C++ standard, Seastar, Boost, project headers.
- Ask "are all these includes needed?" when seeing new includes.
- **Avoid creating small standalone `.cc` files** for single functions or extensions. Consolidate code into existing files that already use the same functionality.
- Test-only accessors should not be added to production headers (e.g., `sstables/sstables.hh`). Place them in test utility headers like `test/lib/sstable_utils.hh`.
 
### 6. Error Handling and Logging
 
- Prefer `on_internal_error()` over `SCYLLA_ASSERT` for internal consistency checks. Reserve `SCYLLA_ASSERT` for truly impossible states.
- Don't parse error message strings (e.g., "Address already in use") for control flow; this breaks with `$LANG` changes.
- Log messages must include sufficient context (table name, tablet ID, operation).
- Verify that log format strings have matching formatter placeholders for all arguments. Missing placeholders silently swallow information.
- When code logic changes, verify that surrounding log/print messages still accurately describe what happens.
- Don't change error codes without justification.
- When introducing new error/exception types, ensure they integrate with the driver protocol (e.g., timeout exceptions must allow driver retries).
 
### 7. Testing
 
#### Test Quality
- Tests must be **fast**. Avoid sleeps; use condition polling.
- Tests must be **stable**. New tests must pass 100/100 runs (`--repeat 100 --max-failures 1`).
- Tests must be **minimal**: use the fewest nodes possible. Prefer single-node if sufficient.
- Tests must be **deterministic**: no random input that makes results unpredictable.
- Tests must be **focused**: test one thing and one thing only per unit test.
- Tests for bug fixes must: reference the issue (GitHub or Jira), demonstrate the failure before the fix, and pass after the fix.
 
#### Test Design
- Prefer unit tests over cluster tests when possible. Cluster tests are expensive in CI. If a test can be written as a unit test (e.g., in `tablets_test.cc`), don't make it a cluster test.
- Verify effects, not implementation: check that data was written, not that a log message appeared.
- Don't create overly generic test utilities that won't be reused. Three similar lines is better than a premature abstraction.
- Don't parametrize the number of nodes in tests. The node count usually cannot be arbitrarily changed and depends on the test scenario (e.g., 3 nodes when testing quorum loss). Don't create generic "scaling" fixtures that parametrize resources.
- Question whether tests for cosmetic changes (e.g., log level changes) warrant a full cluster test. Prefer lighter test types when possible.
- Check whether a new test creates a "bisect trap" (a test that fails on the commit before the fix, making `git bisect` unreliable).
- Don't paper over infrastructure problems at the test level (e.g., reducing node counts because CI machines are too small). Fix the infra problem or skip the test in the affected mode.
- Ask: "Is this test worth the CI cost?" Especially for tests that only check cosmetic behavior (e.g., log level changes).
- For test/cqlpy and test/alternator: consider running against Cassandra/DynamoDB to verify compatibility.
- Flag `xfail` markers that are missing issue references.
- Don't duplicate test infrastructure: reuse existing fixtures and helpers (e.g., `test/lib/eventually.hh`, `wait_for_cql()`).
- Before writing new utility code, search for existing implementations. Fix existing utilities rather than creating parallel ones (e.g., use `check_not_aborted()` instead of manual abort checks, fix `local()` rather than adding `is_local_index()`).
- When refactoring test code to use different helpers (e.g., replacing `make_default_schema()` with `simple_schema()`), verify they are actually equivalent. Test refactoring can silently change what is being tested.
- Write tests **now**, not as a TODO for later. If a test for a known bug would currently fail, add it with `xfail` and reference the issue.
- For Python tests using sleeps for waiting: replace with polling loops that check a condition and sleep briefly between checks. This makes tests more resilient to CPU starvation.
 
#### Debug Mode Considerations
- Tests in debug are always slower. Reduce iterations, rows, data volume in debug mode.
- Scale-down logic must be per-test, not a generic global factor.
 
### 8. Commits and PR Hygiene
 
#### Commit Messages
- Title: concise, imperative mood, prefixed with subsystem (e.g., `raft: fix leader election race`).
- Body: explain **why** the change is needed, not just what changed.
- Reference issues: use `Fixes #NNN` or `Refs #NNN`.
- Backport labels: justify each one. Don't backport if the bug has never manifested in production.
- Commit messages and code must be consistent. If the commit says "fix X" but the code does Y, flag it.
 
#### Commit Structure
- Each commit must compile and pass tests independently (bisectability).
- Separate logical changes into separate commits: don't mix refactoring with bug fixes.
- Don't mix formatting-only changes with functional changes.
- When a previous commit in the series introduces a formatting change, the description should be "Fix formatting after previous patch".
- Don't introduce code in one commit that is immediately rewritten in the next commit in the same series.
- If a patch adds a variable or function that a previous patch in the series already references, the patches are mis-ordered.
 
#### PR Scope
- Flag PRs that try to do too much. Suggest splitting into focused PRs.
- Suggest moving tangential improvements to follow-up PRs.
- When removing code, verify nothing references it. Flag empty files left behind.
 
### 9. Documentation and Comments
 
- Comments explain **why**, not **what**. Code should be self-documenting through clear naming.
- Update comments when code changes. Flag stale/outdated comments.
- Commit messages that reference PRs instead of issues are wrong: PR #NNN is not the same as issue #NNN.
- JIRA references belong in the commit body, not in the commit title.
- For complex data structures: document invariants at the declaration site.
- When type names create confusing hierarchies (e.g., `commitlog_entry` vs `mutation_entry` vs `commitlog_entry_variant`), require explanatory comments at the **declaration site** of each type, not just at usage sites.
- When new config options are added, they should document what happens when modified at runtime.
 
### 10. API and Design
 
- Significant features or architectural changes require a design document and testing report before approval. Ask for them if missing.
- Don't add unnecessary wrappers; if main.cc has direct access to a service, use it directly.
- Don't introduce new concepts (like "primary replicas") without proper design discussion.
- When adding new fields to frequently-instantiated structures (like `tablet_info`), consider memory impact. Use `tablet_transition_info` for transition-only data.
- Avoid bloating popular headers with new includes.
- When de-futurizing a function, also de-futurize its callers if they become synchronous too.
- After removing old code paths (e.g., v1 of an API), clean up suffixes (`_v2` should become the new name).
- Verify co-located table handling (LWT, same-pk materialized views) when working with tablet scheduling.
 
### 10b. AI-Generated Code
 
- Scrutinize code that appears to be AI-generated for hallucinated APIs, incorrect behavior claims, or fabricated function signatures. AI tools can confidently describe nonexistent behavior.
- Verify all API calls and library function usages actually exist and behave as described in comments.
 
### 10c. Backward Compatibility
 
- Any change to serialized formats, RPC messages, or persistent data must consider upgrade/downgrade paths and rolling restart scenarios.
- Changes that affect ARN formats, table names, or wire protocols can break rolling upgrades when some nodes are upgraded and some are not.
- Distinguish between transient state (safe to change) and persistent config (requires migration path).
 
### 11. Python-Specific (Tests)
 
- Use context managers instead of manual cleanup patterns. Context managers **must** use `try`/`finally` around `yield` to ensure cleanup runs even when user code raises exceptions.
- Use `async`/`await`, not callback-style.
- Don't import dtest compatibility layer functionality into cluster tests.
- Names starting with underscore have meaning in Python; don't use `_` prefix without reason.
- Prefer `pytest` fixtures over manual setup/teardown.
- Share test clusters via session-scoped fixtures to reduce test time.
- Check that test helpers return values instead of using side effects when possible.
- Avoid nested `except`-`raise`-`except` anti-patterns; merge into a single `except` block when possible.
 
### 12. Backporting
 
- Justify each backport label. If the bug was introduced recently and isn't in released branches, don't backport.
- If forwarding has always been enabled for a code path, the bug may never have manifested in older versions.
- Backport justification must explain the risk and impact, not just "it's a bug".
 
## Review Output Format
 
Structure your review as follows:
 
1. **Summary**: One paragraph describing what the PR does and your overall assessment.
2. **Critical Issues**: Bugs, correctness problems, crash risks, data loss risks. These block merging.
3. **Improvements**: Performance issues, design concerns, naming problems. These should be addressed but may not block.
4. **Nits**: Style, formatting, typos. Nice to fix but non-blocking.
5. **Questions**: Things you're unsure about. Ask rather than assume.
 
For each comment:
- Reference the specific file and line.
- Quote the relevant code.
- Explain what's wrong and why.
- Suggest a concrete fix when possible.

