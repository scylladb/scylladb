# Integrating ScyllaDB Reviewer Skill with GitHub Copilot

This guide explains how to integrate the ScyllaDB reviewer skill with GitHub Copilot or other AI coding assistants.

## Overview

The ScyllaDB reviewer skill provides structured guidance for automated code reviews. It captures the review patterns and expertise of ScyllaDB maintainers through analysis of 200+ pull requests.

## File Structure

```
.github/
├── copilot-instructions.md          # Main instructions (references reviewer skill)
└── instructions/
    ├── README.md                     # Documentation overview
    ├── reviewer.instructions.md      # Main reviewer skill (21KB)
    ├── review-checklist.md          # Quick reference (3KB)
    ├── example-review.md            # Example usage (8KB)
    ├── cpp.instructions.md          # C++ guidelines
    └── python.instructions.md       # Python guidelines
```

## Integration Methods

### Method 1: GitHub Copilot for Pull Requests (Recommended)

GitHub Copilot automatically uses instruction files in `.github/` when reviewing PRs.

**Setup:**
1. Instruction files are already in `.github/instructions/`
2. Copilot will automatically reference them when assigned to PRs
3. No additional configuration needed

**Usage:**
1. Open a PR in the scylladb/scylladb repository
2. Assign @copilot as a reviewer or use `/copilot review`
3. Copilot will use the reviewer skill to analyze the changes
4. Review comments will follow the P0/P1/P2 priority structure

**Expected Behavior:**
- Copilot will catch P0 issues (async violations, memory issues, test problems)
- Feedback will include specific examples and references to guidelines
- Comments will use the educational tone from the skill
- Cross-references to similar patterns in the codebase

### Method 2: Manual Context Loading

For local usage or other AI assistants:

**With Claude/ChatGPT:**
```
Load the following context before reviewing:
1. .github/instructions/reviewer.instructions.md
2. .github/instructions/cpp.instructions.md (for C++ code)
3. .github/instructions/python.instructions.md (for Python code)

Then follow the 3-phase review workflow from reviewer.instructions.md
```

**Example prompt:**
```
Review this pull request using the ScyllaDB reviewer skill:
[paste PR diff]

Check for:
1. P0: Critical issues (async violations, memory issues, test quality)
2. P1: Design issues (naming, error handling, resource management)
3. P2: Polish (style, documentation, organization)

Use feedback templates and examples from reviewer.instructions.md
```

### Method 3: CI/CD Integration

For automated checks in CI pipeline:

**Option A: GitHub Actions**
```yaml
name: Copilot Review
on: [pull_request]

jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Request Copilot Review
        run: gh pr review --comment -b "@copilot review"
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

**Option B: Pre-commit Hook**
```bash
# .git/hooks/pre-commit
#!/bin/bash
# Local review before committing
copilot review-changes --skill=.github/instructions/reviewer.instructions.md
```

## Review Workflow

### Phase 1: Critical Issues (P0)
**Time: ~5 minutes**
**Focus:** Issues that can cause crashes or outages

Copilot will scan for:
1. `.get()` calls on futures (blocks reactor)
2. Exceptions in hot paths (performance)
3. Raw `new`/`delete` (memory safety)
4. Hardcoded `sleep()` in tests (flakiness)

**Action:** If P0 issues found, request fixes before proceeding

### Phase 2: Design Review (P1)
**Time: ~10 minutes**
**Focus:** Maintainability and correctness

Copilot will check:
1. API naming and design
2. Error handling completeness
3. Resource management patterns
4. Test coverage

### Phase 3: Polish (P2)
**Time: ~5 minutes**
**Focus:** Code quality and style

Copilot will note:
1. Style and formatting
2. Documentation quality
3. Code organization
4. Minor optimizations

## Expected Output Format

Reviews will follow this structure:

```markdown
## Code Review Summary

### Critical Issues (P0) - Must Fix
- [Location] Issue description with impact
- Specific fix with code example
- Reference to guideline section

### High Priority (P1) - Should Fix
- [Location] Issue description
- Suggested improvement
- Alternative approaches

### Medium Priority (P2) - Nice to Fix
- nit: Style issue
- Documentation suggestion

### Overall Assessment
BLOCKING / APPROVED / APPROVED WITH COMMENTS
```

## Customization

### Adding Custom Patterns

To add project-specific patterns:

1. Edit `.github/instructions/reviewer.instructions.md`
2. Add new pattern under appropriate priority level (P0/P1/P2)
3. Include example code and feedback template
4. Update `.github/instructions/review-checklist.md`

Example:
```markdown
### X. Your Custom Pattern (PRIORITY)
**Check for:**
- Specific anti-pattern

**Example:**
```cpp
❌ Bad example
✅ Good example
```

**Feedback Template:**
```
[Explanation and fix]
```
```

### Adjusting Priority Levels

If certain patterns become more/less critical:

1. Move pattern to different priority section
2. Update checklist accordingly
3. Document reason in commit message

### Updating Based on Feedback

Track false positives and adjust:

```markdown
## Tracking Metrics
- False Positive Rate: Comments dismissed by authors
- Actionability Rate: Comments leading to changes
- Category Distribution: Most common issues

Use metrics to refine patterns and priorities.
```

## Best Practices

### For AI Reviewers

1. **Always start with P0 checks** - Most critical issues first
2. **Provide specific examples** - Not just "this is wrong"
3. **Reference guidelines** - Link to relevant sections
4. **Suggest alternatives** - Don't just critique
5. **Use appropriate tone** - Educational, not harsh
6. **Flag for human review** - When uncertain

### For Human Reviewers Using the Skill

1. **Use as a checklist** - Systematically check each category
2. **Focus on what matters** - P0 > P1 > P2
3. **Learn the mantras** - Internalize ScyllaDB principles
4. **Reference examples** - Point to similar patterns
5. **Provide context** - Explain why things matter

### For Contributors

1. **Self-review first** - Use checklist before submitting
2. **Study examples** - Learn from example-review.md
3. **Know the mantras** - Understand project philosophy
4. **Ask questions** - If feedback is unclear
5. **Learn patterns** - Avoid repeating issues

## Maintenance

### Quarterly Updates

Review and update based on:
- New patterns emerging in PRs
- Changes in project architecture
- Evolving best practices
- Feedback from maintainers

### Metrics to Track

1. **Coverage**: % of PRs reviewed by skill
2. **Accuracy**: % of comments that lead to fixes
3. **False Positives**: % of comments dismissed
4. **Time Saved**: Maintainer review time reduction
5. **Quality**: Severity of issues caught

### When to Update

- **Immediately**: If critical pattern causes false positives
- **Weekly**: Add patterns from recent PR reviews
- **Quarterly**: Major refresh based on metrics
- **As needed**: After architectural changes

## Troubleshooting

### Issue: Too Many False Positives

**Solution:** 
1. Review patterns causing false positives
2. Add more specific conditions or examples
3. Consider moving pattern to lower priority
4. Add exceptions for valid use cases

### Issue: Missing Important Issues

**Solution:**
1. Analyze PRs where issues were missed
2. Add pattern to appropriate priority level
3. Update checklist and examples
4. Test on historical PRs

### Issue: Comments Too Generic

**Solution:**
1. Enhance feedback templates with specifics
2. Add more code examples
3. Include references to similar patterns
4. Link to relevant documentation

### Issue: Integration Not Working

**Solution:**
1. Verify files are in `.github/instructions/`
2. Check file permissions and format
3. Review GitHub Copilot documentation
4. Test with simple example PR first

## Testing the Skill

### Test with Example PR

1. Create a test branch with known issues:
```bash
git checkout -b test-reviewer-skill
# Add code with known P0/P1/P2 issues
git commit -m "test: code with review issues"
git push
```

2. Open PR and request Copilot review

3. Verify Copilot catches:
   - P0: Blocking async operations
   - P0: Memory management issues
   - P1: Naming problems
   - P2: Style issues

4. Check feedback quality:
   - Specific and actionable
   - Includes code examples
   - References guidelines
   - Appropriate priority

### Validation Checklist

- [ ] Copilot references reviewer skill in comments
- [ ] P0 issues are caught and flagged as blocking
- [ ] Feedback includes specific fixes with examples
- [ ] Comments reference guideline sections
- [ ] Tone is educational and helpful
- [ ] False positive rate is acceptable (<10%)
- [ ] Coverage includes all file types (C++, Python)

## Resources

### Documentation
- Main skill: `.github/instructions/reviewer.instructions.md`
- Quick reference: `.github/instructions/review-checklist.md`
- Examples: `.github/instructions/example-review.md`
- C++ guidelines: `.github/instructions/cpp.instructions.md`
- Python guidelines: `.github/instructions/python.instructions.md`

### Analysis Source
- Original analysis: `~/.copilot/session-state/scylladb-review-analysis/`
- PR patterns: `scylladb_pr_review_patterns.md`
- Recommendations: `reviewer_skill_recommendations.md`
- Statistics: `scylladb_prs_analyzed_summary.md`

### External Resources
- GitHub Copilot docs: https://docs.github.com/copilot
- Claude skills guide: Anthropic's skill building guide
- ScyllaDB contribution guide: `CONTRIBUTING.md`

## Support

For questions or issues:
1. Check this integration guide first
2. Review example-review.md for usage examples
3. Consult README.md for overview
4. Open issue in scylladb/scylladb repository
5. Tag relevant maintainers

---

**Version:** 1.0  
**Last Updated:** February 2026  
**Next Review:** May 2026
