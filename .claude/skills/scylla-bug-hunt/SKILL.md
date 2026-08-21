---
name: scylla-bug-hunt
description: >
  Systematic, subsystem-by-subsystem audit of the ScyllaDB C++ codebase for
  (1) logical bugs, (2) performance issues, and (3) scale issues — code that
  works today but degrades badly at high object counts (1000s of tables,
  100,000s of tablets, 100,000s of sstables per node, 10,000s of concurrent
  CQL connections). Fans out codegraph-backed research subagents per
  module/submodule (compaction: ICS/TWCS/STCS/LCS, repair: incremental vs
  legacy, CQL: parser/authz/functions, memtables & cache, read/write path,
  ...), triages and dedups the candidates, builds a git worktree + branch +
  a *failing* reproducer (boost test, cqlpy, or test.py) for each surviving
  candidate to prove it's real, then writes an impact/risk/complexity bug
  report. Use whenever the user asks to audit, sweep, or systematically hunt
  a ScyllaDB module or submodule for bugs/perf/scale problems, or asks
  "what breaks at scale in X". Do NOT use this for reviewing a specific PR
  or diff (use the code-review skill instead) or for chasing a single
  already-known bug with a reproducer in hand (just fix it, or use
  autoresearch:debug).
---

# ScyllaDB bug hunt

A five-stage pipeline, run through the `Workflow` tool, that turns "go look
at compaction for bugs" into a small number of **worktrees, each with a
branch and a reproducer that already fails**, plus a written case for why
each one matters. It exists because ad-hoc reading of a 100k-line subsystem
either misses the scale-shaped bugs (they don't look wrong at N=10) or
finds too many low-value maybes to act on. The pipeline forces every
candidate to survive triage, then survive an actual reproduction attempt,
before anyone spends time writing it up.

Read `references/bug-patterns.md` before scoping any run — it has the
concrete "does this actually bite at scale" checklist and the four
reference scale points, and it should shape the research prompt, not just
sit as background reading. `references/module-map.md` grounds module names
in real top-level directories. `references/report-template.md` is the exact
structure the final writeup must follow.

## When invoked

1. **Resolve scope.** Figure out the module (and, if the user named one,
   the submodule) from the request. If the user said "compaction," that's
   a module; ICS/TWCS/incremental-repair/CQL-parser-level asks are
   submodules — see `references/module-map.md`. If nothing narrower than
   "the codebase" was said, don't run this skill against everything at
   once: ask which module to start with, or pick the module the user has
   been actively working in per recent context.
2. **Check for a resumable run first.** Look in `runs/` (next to this
   file) for a manifest matching this module whose `status` isn't
   `"completed"`. If one exists, this is a restart after an interruption
   (ran out of tokens/context, session died, workflow errored) — resume it
   with the *exact* `scriptPath`/`args` recorded in the manifest plus
   `resumeFromRunId`, and tell the user you're resuming run `<runId>`
   rather than starting over. Skip sizing/confirmation in that case; it
   already happened once. If the user is explicitly asking for a different
   scope than the stale manifest records, archive it (rename with a
   `.stale` suffix, don't delete — it may still hold a useful `runId` to
   inspect) and proceed as a fresh run.
3. **Size the run before starting it** (fresh runs only). This pipeline
   compiles and runs real C++ (boost tests, sometimes a full scylla binary
   for cqlpy). Tell the user roughly how many submodules and how many
   reproduction candidates (`maxReproduce`, default 3) you're about to
   spin up, and confirm if it's the user's first run of this skill or the
   scope is large (a whole module with many submodules). Default
   `maxReproduce` to 3 unless they ask for more — reproduction is the
   expensive, build-bound stage, and a wide net there risks exactly the
   "other build on the host fights this one for CPU" contamination the
   user has hit before with perf benchmarking.
4. **Run the workflow, and checkpoint it immediately**:
   ```
   Workflow({
     scriptPath: ".claude/skills/scylla-bug-hunt/workflow.js",
     args: {
       module: "<module name, e.g. 'compaction'>",
       submodules: [<optional explicit list; omit to let Scope discover it>],
       maxReproduce: 3,
     },
   })
   ```
   Invoking this skill is itself the user's explicit request for
   sub-agent orchestration — don't ask again whether it's OK to use
   `Workflow`. As soon as the call returns a Task ID / Run ID, write
   `runs/<module>.json` (see "Checkpointing across sessions" below)
   *before* doing anything else — that write is what makes the run
   recoverable if this very session ends a moment later.
5. **Present the result, then stop.** The workflow returns confirmed bugs
   (each with a worktree path, branch, reproducer, and the impact/risk/
   complexity writeup) plus a short list of what was discarded and why
   (dedup, unreproduced, low-confidence). Summarize this for the user, and
   mark the manifest `"completed"` (or delete it) now that there's nothing
   left to resume. **Do not start fixing, committing beyond the
   reproducer, pushing, or opening a PR** — this skill's job ends at
   "here's a proven bug and what it costs us," matching the user's
   explicit "we'll then proceed from there." Any of those next actions
   need a separate, explicit ask, and a push/PR needs explicit approval
   regardless of what earlier turns approved.

## Why this shape (read once, not per run)

- **Model per stage, not one model for everything.** Research fans out
  wide (one agent per submodule) and just needs to be a good bug-shaped
  pattern matcher over code codegraph hands it — that's Sonnet, high
  effort. Triage and impact writeup are low-volume, high-stakes judgment
  calls over material someone else already gathered — that's Opus, high
  effort. Scoping is a mechanical enumeration — cheap model, low effort.
  This is what "optimize token consumption" cashes out to: spend the
  expensive model where the call count is low and the judgment matters,
  not on every parallel research call.
- **Compaction between stages is structural, not a step you add.** Every
  stage boundary is an `agent()` call returning a schema-validated object.
  The subagent's raw exploration — every codegraph query, every file read,
  every dead end — stays in that subagent's own context and disappears
  when it returns. Only the distilled result (a candidate list, a triage
  verdict, a reproduction outcome) crosses into the next stage. That's the
  compaction the user asked for; there's no separate "now compact" step to
  design.
- **Reproduce before you argue impact.** A hypothesis that can't be turned
  into a failing test or a measured super-linear growth curve isn't a bug
  report yet, it's a hunch — don't let it consume an Opus impact-writeup
  call. The pipeline enforces the order: reproduce, *then* write up.
- **Scale bugs need a scale-shaped reproducer, not a scale-shaped
  cluster.** Nobody is provisioning 100,000 real tablets in a test. The
  reproducer should isolate the pathological data structure or algorithm
  and show its cost growing the wrong way between a small N and a larger
  but still test-feasible N (e.g., assert the op count or wall-clock ratio
  between N=100 and N=10,000 is roughly linear-in-N when it should be
  constant or log(N)). `references/bug-patterns.md` has examples.

## Checkpointing across sessions

A full audit of a module can run long enough to outlast the session that
started it — the run itself must survive that, not just be restartable
from zero. There are two layers, and only the second one needs anything
from you:

- **Free, built into `Workflow`:** every `agent()` call is cached by its
  exact `(prompt, opts)`. Calling `Workflow({scriptPath, resumeFromRunId})`
  with the same `scriptPath`/`args` replays every already-finished stage
  instantly — Scope, Research, Triage, and any candidate whose worktree +
  reproducer already got built — and only continues the first call that
  hadn't finished yet. This is what makes resuming cheap instead of
  redoing the expensive (build-bound) Reproduce stage from scratch.
- **Not free, and your job:** nothing durable records *which* `runId`
  belongs to *which* module unless you write it down. The running task and
  its `runId` live in this conversation and in `/workflows` — both gone if
  the session ends. So immediately after launching (step 4 above), write
  a manifest:

  ```json
  // runs/<module>.json
  {
    "module": "alternator",
    "scriptPath": ".claude/skills/scylla-bug-hunt/workflow.js",
    "args": { "module": "alternator", "maxReproduce": 3 },
    "runId": "wf_...",
    "taskId": "...",
    "status": "running",
    "started": "<UTC timestamp>"
  }
  ```

  This is disposable run bookkeeping, not project knowledge — it belongs
  in `runs/` on disk (gitignored, see `runs/.gitignore`), never in the
  auto-memory system. A brand-new session with zero memory of this
  conversation can still find it by module name, pass its `runId` to
  `Workflow`, and pick up exactly where things stopped. Update its
  `status` to `"completed"` when you present the final result (step 5) so
  a later run on the same module doesn't get mistaken for a resume.

## Boundaries

- Local git worktrees and branches only, under `.worktrees/` following this
  repo's existing naming convention (see the branches already there for
  style). Never push, never open a PR, never touch `master` or any shared
  branch.
- Never touch vnode/legacy repair paths — if the repair module comes up,
  gate everything on `uses_tablets()` / the tablet-based incremental path.
- If a reproduction agent finds another build already running on this
  host (check for a live `ninja`/`dbuild` process), it should wait rather
  than pile on; concurrent builds skew both its own timing measurements
  and anything else running on the box.
- If triage or reproduction comes back mostly empty for a submodule, say so
  plainly — don't pad the report, and don't quietly drop candidates without
  listing why they didn't make it.
