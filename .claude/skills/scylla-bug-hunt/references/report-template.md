# Bug report structure

The Impact stage must produce exactly this structure for every confirmed
finding. Keep it tight — this is a triage document for the user to decide
what to do next with, not a design doc.

```markdown
# <short title>

**Category:** logical | performance | scale
**Module / submodule:** <e.g. compaction / ICS>
**Worktree:** <path under .worktrees/>
**Branch:** <branch name>
**Reproducer:** <path to the test> — `<how to run it>`

## Summary
One or two sentences: what's wrong, in plain terms.

## Evidence
What the reproducer actually shows (failing assertion, or the measured
growth curve/ratio for a scale finding). Include the concrete numbers, not
just "it's slower."

## Impact
Who is affected and under what conditions (which of the four scale points,
if any; correctness vs availability vs latency vs resource exhaustion).
Be concrete about the threshold where it starts to matter, not just "at
scale."

## Risk
Likelihood of hitting this in a real deployment, and blast radius if it
does (single request, single shard, whole node, whole cluster). Distinguish
"already biting someone" from "will bite once X grows."

## Complexity
Rough sense of how hard a real fix is, and whether the fix itself carries
risk (touches a hot path, changes on-disk/wire format, needs a migration).

## Recommendation
One line: fix now, fix later, needs more investigation, or not worth it
(and why, if triage almost dropped it but a reproducer justified keeping it).
```

## Discarded-candidate note

For anything triaged out or that failed to reproduce, one line each in the
final summary is enough: title, and why (duplicate of X, low confidence,
reproducer didn't confirm the hypothesis — say what it showed instead).
Never drop these silently; the point is showing what was covered, not just
what stuck.
