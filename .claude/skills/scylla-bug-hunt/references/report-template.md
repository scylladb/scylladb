# Bug report structure

The Impact stage must produce exactly this structure for every confirmed
finding. Keep it tight — this is a triage document for the user to decide
what to do next with, not a design doc.

Confidence and severity are two different axes and both are required —
don't collapse them into one rating. Confidence is about the hunt, not the
bug: how sure this is real and not a misreading of the code, independent of
how bad it'd be. Severity is about the bug, not the hunt: how bad it is if
real, independent of how sure anyone is. A low-confidence, critical-severity
finding and a high-confidence, low-severity one are both valid and belong
on the report with their ratings kept apart. Severity follows the same
impact reasoning as the Impact/Risk sections below: `critical` — data loss,
corruption, or cluster-wide unavailability; `high` — availability/latency
impact at a single node or shard, or the four scale reference points once
crossed; `medium` — degraded performance or a narrow correctness edge case
short of data loss; `low` — cosmetic, or only matters far outside the four
scale reference points.

```markdown
# <short title>

**Category:** logical | performance | scale
**Confidence:** low | medium | high — how sure the hunt is this is a real bug, not a false positive; justify in one clause (e.g. "high — reproducer fails deterministically on current master")
**Severity:** critical | high | medium | low — impact if real (see Impact/Risk below for the reasoning; this is the one-word rating)
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
