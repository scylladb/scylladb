# ScyllaDB Code Review Checklist

Quick reference for code reviewers. See `reviewer.instructions.md` for detailed guidance.

---

## ⚠️ Critical (P0) - Must Fix Before Merge

### Async/Seastar Violations
- [ ] No `.get()` calls on futures (blocks reactor)
- [ ] All async operations use `co_await`
- [ ] No blocking I/O operations
- [ ] No `std::mutex` (use `seastar::semaphore`)

### Exception Handling
- [ ] No exceptions in hot paths (use `std::expected` or results)
- [ ] No exceptions for control flow
- [ ] Correct `noexcept` specifications
- [ ] Exceptions properly handled in coroutines

### Memory Management
- [ ] No raw `new`/`delete` (use smart pointers)
- [ ] Large objects passed by `const&` or `&&`
- [ ] Pre-allocation when size is known
- [ ] Correct smart pointer type (`lw_shared_ptr` vs `shared_ptr`)

### Test Quality
- [ ] No hardcoded `sleep()` (use proper synchronization)
- [ ] Consistency levels specified (CL=ALL for setup)
- [ ] Test validates the actual fix
- [ ] Test runs in correct mode (debug/release guards)

---

## ⚡ High Priority (P1) - Should Fix

### Naming & API Design
- [ ] Function names are specific, not generic
- [ ] Consistent verb usage in related functions
- [ ] Functions in appropriate namespace
- [ ] Clear API boundaries

### Error Handling
- [ ] All function calls checked for errors
- [ ] Null pointer checks before dereferencing
- [ ] Errors logged with context
- [ ] No silently ignored errors

### Resource Management
- [ ] RAII patterns used throughout
- [ ] No manual resource management
- [ ] Resources cleaned up in error paths
- [ ] Appropriate smart pointer usage

### Test Coverage
- [ ] Bug fixes include tests
- [ ] New features have tests
- [ ] Negative test cases included
- [ ] Edge cases covered

---

## 📋 Medium Priority (P2) - Nice to Fix

### Code Style
- [ ] Spaces after commas
- [ ] Consistent formatting
- [ ] `fmt` instead of streams
- [ ] Python follows PEP 8

### Documentation
- [ ] Comments explain "why", not "what"
- [ ] No obvious comments
- [ ] Preconditions documented
- [ ] Public APIs documented

### Code Organization
- [ ] Commit messages have subsystem prefixes
- [ ] Related changes together
- [ ] No copy-paste without refactoring
- [ ] Appropriate commit structure

---

## 🎯 Quick Spot Checks

**In every PR, quickly scan for:**

1. Any `.get()` on a future → P0
2. Exception thrown in a loop → P0
3. Raw `new` or `delete` → P0
4. `sleep()` in a test → P0
5. Generic function name like `process()` → P1
6. Missing error check → P1
7. Manual resource management → P1
8. Missing test for bug fix → P1

---

## 📚 Reference Mantras

1. "Make it obvious" - Self-documenting code
2. "Don't block the reactor" - Always async
3. "Keep commits bisectable"
4. "Test what you fix"
5. "Subsystem prefixes matter"
6. "Don't allocate in hot paths"
7. "RAII everything"
8. "Fail fast with context"
9. "One fiber per connection"
10. "Results over exceptions in data path"

---

## 🔗 Related Guidelines

- Full details: `.github/instructions/reviewer.instructions.md`
- C++ patterns: `.github/instructions/cpp.instructions.md`
- Python patterns: `.github/instructions/python.instructions.md`
- Build & test: `.github/copilot-instructions.md`

---

**Quick Tip:** Start with P0 issues. If any found, request fixes before deeper review.
