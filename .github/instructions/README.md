# ScyllaDB Reviewer Skill - Documentation

## Overview

This directory contains the **ScyllaDB Code Review Skill** - a comprehensive guide for performing in-depth code reviews similar to ScyllaDB maintainers. The skill is based on analysis of 200+ pull requests and 700+ review comments from ScyllaDB maintainers.

## Purpose

The reviewer skill helps:
1. **Prevent critical bugs** from merging (async violations, memory issues, etc.)
2. **Educate contributors** about ScyllaDB patterns and best practices
3. **Reduce maintainer burden** by catching repetitive issues automatically
4. **Maintain consistent** code quality standards across the project

## Files

### 1. `reviewer.instructions.md` (Primary Document)
**21 KB** - Complete code review skill with:
- Prioritized review checklist (P0/P1/P2)
- Common anti-patterns with examples
- Feedback templates for each issue type
- Review workflow and guidelines
- Integration with existing guidelines

**When to use:** As the comprehensive reference for code reviews

### 2. `review-checklist.md` (Quick Reference)
**3 KB** - Condensed checklist format:
- Critical issues (P0)
- High priority issues (P1)
- Medium priority issues (P2)
- Quick spot checks
- Key mantras

**When to use:** During active reviews for quick reference

### 3. `cpp.instructions.md`
C++-specific coding guidelines including:
- Memory management patterns
- Seastar asynchronous programming
- Performance considerations
- Style guidelines

### 4. `python.instructions.md`
Python-specific coding guidelines including:
- PEP 8 style
- Testing patterns
- Import organization
- Documentation standards

## Key Findings from Analysis

### Top 4 Critical Patterns (P0)
1. **Async/Seastar Violations** - Can crash the reactor
2. **Exception Handling in Data Path** - Performance and correctness issues
3. **Memory Management Issues** - Safety violations and leaks
4. **Test Quality Issues** - Flakiness and reliability problems

### Top 10 Reviewer Mantras
1. "Make it obvious" - Self-documenting code
2. "Don't block the reactor" - Always async
3. "Keep commits bisectable"
4. "Test what you fix"
5. "Subsystem prefixes matter"
6. "Don't allocate in hot paths"
7. "RAII everything"
8. "Fail fast with context"
9. "One fiber per connection"
10. "Results over exceptions (in data path)"

### Most Active Reviewers Analyzed
- **avikivity**: Performance, async patterns, test quality
- **denesb**: Reader concurrency, memory, correctness
- **bhalevy**: Style, noexcept, resource management
- **tgrabiec**: Architecture, load balancing
- **nyh**: Naming, API clarity
- **patjed41**: Testing, Python, edge cases

## Usage

### For AI Code Reviewers (GitHub Copilot, etc.)
1. Load `reviewer.instructions.md` as context when reviewing PRs
2. Follow the 3-phase review workflow:
   - Phase 1: Critical issues (P0)
   - Phase 2: Design review (P1)
   - Phase 3: Polish (P2)
3. Use provided feedback templates
4. Reference existing guidelines from `cpp.instructions.md` and `python.instructions.md`

### For Human Reviewers
1. Quick reference: Use `review-checklist.md` during reviews
2. Detailed guidance: Refer to `reviewer.instructions.md` for examples
3. Learning: Study the patterns to understand ScyllaDB review culture

### For Contributors
1. Self-review your PRs using the checklist before submitting
2. Learn common anti-patterns to avoid
3. Study the mantras to understand ScyllaDB principles

## Integration with CI/CD

The reviewer skill is designed to be used by GitHub Copilot or similar AI coding assistants:

1. **Automatic Assignment**: Copilot can be assigned to review all PRs
2. **Structured Feedback**: Uses priority levels (P0/P1/P2) for triage
3. **Educational**: Provides context and examples with feedback
4. **Actionable**: Includes specific suggestions and code examples

## Maintenance

This skill should be updated:
- **Quarterly** - To capture evolving patterns
- **After major changes** - When architecture or practices change significantly
- **When patterns shift** - If review feedback trends change
- **New reviewers** - When new maintainers join with different focus areas

## Statistics (Base Analysis)

- **PRs Examined:** ~200 merged PRs
- **Detailed Analysis:** 16 PRs with 40+ comments each
- **Comments Analyzed:** 700+
- **Time Period:** Q4 2025 - Q1 2026
- **Pattern Categories:** 16 major categories identified

## Examples of Issues Caught

### P0 Example: Blocking Async Operation
```cpp
// ❌ Blocks the reactor
auto result = some_future().get();

// ✅ Correct
auto result = co_await some_future();
```

### P0 Example: Exception in Hot Path
```cpp
// ❌ Performance issue
for (auto& row : rows) {
    if (!valid(row)) throw invalid_row();
}

// ✅ Better approach
for (auto& row : rows) {
    if (!valid(row)) return std::unexpected(error_code::invalid_row);
}
```

### P1 Example: Generic Naming
```cpp
// ❌ Too generic
json to_json(const data& d);

// ✅ Specific
json to_vector_search_json(const restrictions& r);
```

### P0 Example: Flaky Test
```python
# ❌ Race condition
await insert(key, value)
await asyncio.sleep(1)
assert await read(key) == value

# ✅ Synchronized
await insert(key, value, consistency_level=Consistency.ALL)
assert await read(key) == value
```

## Related Resources

- **Main Guidelines**: `../.github/copilot-instructions.md`
- **C++ Guidelines**: `cpp.instructions.md`
- **Python Guidelines**: `python.instructions.md`
- **Original Analysis**: Stored in `~/.copilot/session-state/scylladb-review-analysis/`
  - `scylladb_pr_review_patterns.md` - 16 KB detailed patterns
  - `reviewer_skill_recommendations.md` - 8 KB implementation guide
  - `scylladb_prs_analyzed_summary.md` - 6 KB statistics

## Contributing

To improve this skill:

1. **Report false positives** - Help tune the guidelines
2. **Suggest new patterns** - Add patterns you see repeatedly
3. **Update examples** - Keep code examples current
4. **Add context** - Link to specific PRs demonstrating patterns

## Contact

For questions or suggestions about this reviewer skill:
- Open an issue in the scylladb/scylladb repository
- Tag maintainers who contributed to the patterns
- Reference specific sections from the instructions

---

**Version:** 1.0  
**Created:** February 2026  
**Based on:** Analysis of ScyllaDB PR reviews from Q4 2025 - Q1 2026  
**Last Updated:** February 2026
