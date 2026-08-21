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
   (ran out of tokens/context, session died, workflow errored) — but a
   `"claiming"`/`"running"` manifest doesn't by itself mean the previous
   session is gone, so resuming it must transfer ownership, not just read
   and continue.

   **Owner lease.** Every manifest this session claims or owns carries an
   `owner` object — `{"pid": <shell PID>, "sessionId": "<random id>",
   "heartbeat": "<UTC timestamp>"}` — see the JSON example under
   "Checkpointing across sessions" below. A lease is *expired* once
   `heartbeat` is more than **4 hours** old (or missing entirely, for a
   manifest written before this convention existed); otherwise it's
   *live*. Every place below that says "archive" or "resume" a
   non-`"completed"` manifest means: re-read the manifest fresh from disk
   and check its lease first.

   4 hours, not 30 minutes: the heartbeat only refreshes when a candidate's
   Reproduce/Impact `agent()` call *returns* (see below), and a single call
   can legitimately run that long (a full scylla build plus a boost/cqlpy
   run). A short TTL would let another session steal a still-live lease
   mid-build; there's no way to heartbeat *during* one blocking `agent()`
   call, so the TTL has to outlast the slowest realistic call instead.

   Before resuming: if the lease is live, another session is plausibly
   still working it — do not resume silently; surface the conflict to the
   user and let them decide (wait, or explicitly force a fresh run)
   instead of racing a live session for the same `runId`. If the lease is
   expired, the previous owner is presumed dead: atomically rewrite the
   manifest (temp file + rename in `runs/`, never truncate-in-place) with
   a fresh `owner` (new pid/sessionId) and a refreshed `heartbeat` —
   *before* calling `Workflow` — so ownership is actually transferred, not
   just assumed. Only after that write do you resume with the *exact*
   `scriptPath`/`args` recorded in the manifest plus `resumeFromRunId`,
   and tell the user you're resuming run `<runId>` rather than starting
   over. Skip sizing/confirmation in that case; it already happened once.

   If the user is explicitly asking for a different scope than the stale
   manifest records — a different module, or an explicit `submodules`
   list that doesn't match the manifest's recorded `args.submodules` —
   archive it and proceed as a fresh run, but only once its lease is
   confirmed expired by the same check above; a scope mismatch against a
   manifest with a live lease is still a live session's manifest, and
   archiving it out from under that session is exactly the race this
   lease exists to prevent — surface the conflict to the user instead.
   Archiving means an exclusive, fail-if-exists rename to
   `runs/<key>.json.stale`; if that name is already taken, try `.stale-2`,
   `.stale-3`, ... and use the first one that doesn't already exist —
   never overwrite an existing `.stale*` file, since it may still hold a
   useful `runId` to inspect and a collision means two archives happened
   at once.

   The module name drives the manifest filename everywhere below (this
   step's lookup, the claim below, the resume path) — it's normally a
   short word like "compaction," but never write `runs/<module>.json` with
   the module name substituted verbatim into the path; a name like
   `../../outside` would escape `runs/`, and two different names (e.g.
   `foo/bar` and `foo_bar`) could otherwise sanitize to the same filename
   and overwrite each other's manifest. Compute the manifest filename with
   exactly this shell one-liner before any read, rename, or write, and use
   its output as `<key>` in `runs/<key>.json` — do not hand-derive it a
   different way:
   ```
   key=$(printf '%s' "$module" | tr -c 'A-Za-z0-9_-' '_')
   key="${key:-_}-$(printf '%s' "$module" | cksum | cut -d' ' -f1)"
   ```
   The trailing checksum makes same-key collisions between different
   module names effectively impossible. The full, unsanitized module name
   and the requested `submodules` (if any were given explicitly) still
   belong inside the manifest's `"module"` / `"args"` fields — just not in
   the path — and before treating any manifest found at `runs/<key>.json`
   as a match, confirm its `"module"` field equals the requested module
   name exactly *and*, if this request names an explicit `submodules`
   list, that it matches the manifest's recorded `args.submodules` as a
   set. Either mismatch (a residual key collision, a hand-edited file, or
   a genuinely different scope) means it's not this request's manifest —
   treat it as if none was found (or, per the paragraph above, archive it
   as stale) rather than resuming with the wrong scope silently in effect.

   **Claim the key before doing anything else.** The check above and
   step 4's write are not atomic with each other — two sessions can both
   see "no resumable run" for the same module in the same instant, then
   both proceed, and whichever writes `runs/<key>.json` second stomps the
   other's still-running manifest. Close that window right here, before
   step 3's sizing/confirmation, by writing the full placeholder content
   to a temp file first and only then atomically linking it into place —
   `ln` fails with the target already existing rather than truncating it,
   so a kill mid-write can never leave `runs/<key>.json` half-written:
   ```
   session_id="${RANDOM:-$$}-$$-$(date +%s)"
   now="$(date -u +%FT%TZ)"
   tmp="runs/.${key}.claim.$$"
   printf '{"status":"claiming","owner":{"pid":%d,"sessionId":"%s","heartbeat":"%s"}}' \
     "$$" "$session_id" "$now" > "$tmp"
   claimed=1
   ln "$tmp" "runs/$key.json" 2>/dev/null || claimed=0
   rm -f "$tmp"
   ```
   The `owner`/`heartbeat` written here establishes this session as the
   manifest's lease holder from the very first byte on disk — see "Owner
   lease" above. Refresh the heartbeat (same atomic temp-file-plus-rename
   write, same `owner.pid`/`sessionId`, just a newer timestamp) whenever
   this session writes to a manifest it owns for another reason — the
   step-4 full-manifest overwrite, and, during a long-running Reproduce/
   Impact stage, at least once per candidate that finishes — so a lease
   check by another session sees a live heartbeat for as long as this
   session actually is.

   `claimed=0` means the path was already occupied — re-read
   `runs/$key.json` before assuming a live competitor. Check its lease
   (see "Owner lease" above) regardless of `status`: if the heartbeat is
   expired, the previous owner is presumed dead — treat the manifest as
   abandoned exactly like a `"completed"` one (step 5 leaves those behind
   instead of deleting them): archive it with the same exclusive-rename
   procedure used above for a scope mismatch, then retry the exclusive
   `ln` claim above against the now-free path, which writes this
   session's own `owner` so there's no ambiguity about who holds it next.
   Only a `status` of `"claiming"` or `"running"` *with a live lease*
   means an actual live race: go back to the top of this step (resume it,
   transferring ownership per "Owner lease" above, if it matches this
   module/scope, or surface the conflict to the user if it doesn't — never
   overwrite or archive a manifest whose lease hasn't expired).
   `claimed=1` means you now own `runs/<key>.json`: from here on, only this
   session may write to it — step 4 overwrites it with the full manifest,
   and step 5 marks it `"completed"` — until it's archived or completed.
   A session that lost the claim must not touch the file again. Every
   write to an owned manifest (the placeholder above, step 4's full
   manifest, step 5's `"completed"` update) must be atomic: write the new
   contents to a temp file in `runs/` and rename it over `runs/<key>.json`,
   never truncate-and-write the existing file in place — a session that
   dies mid-write must never leave `runs/<key>.json` holding invalid JSON
   or a truncated manifest.

   **Every release, archive, completion, or overwrite is fenced by the
   owner token.** A heartbeat can expire while this session is legitimately
   still waiting on something (step 3's confirmation, a long Reproduce
   call) — if it does, another session may see the lease as dead and claim
   `runs/<key>.json` for itself. Before this session deletes, archives, or
   overwrites *any* manifest at that path, it must re-read the file fresh
   and check that `owner.sessionId` still equals the `session_id` this
   session generated when it first claimed the key. If it doesn't match,
   someone else now legitimately owns that manifest — leave it alone and
   do not touch it, delete it, or archive it, even if this session
   "started" that path. Only act on the file when the owner token still
   matches. To keep that window small, refresh the heartbeat (same atomic
   temp-file-plus-rename write, same `owner.pid`/`sessionId`, newer
   timestamp) not just when a candidate finishes, but at the start of
   step 3's confirmation prompt and periodically while waiting on it or on
   any other long-running wait state, so a legitimately-active session's
   lease doesn't go stale out from under it.
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

   **If the user declines, or this session ends before step 4 calls
   `Workflow`:** re-read `runs/<key>.json` fresh and check `owner.sessionId`
   against this session's own `session_id` from step 2 (see "Every
   release, archive, completion, or overwrite is fenced by the owner
   token" above) before removing anything — the heartbeat may have expired
   while waiting on this confirmation and another session may already have
   claimed the path. Only `rm` the file if the owner token still matches;
   if it doesn't, another session now owns it and this session must not
   touch it. Do this from whatever turn actually ends the attempt (a
   decline, an error, the user changing their mind) so a retry a minute
   later doesn't read the leftover `"claiming"` placeholder as a live
   competitor for the rest of the lease duration.
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
   `Workflow`. As soon as the call returns a Task ID / Run ID, overwrite
   `runs/<key>.json` — the placeholder you claimed in step 2, now owned by
   this session — with the full manifest, using the sanitized `<key>` from
   step 2 (see "Checkpointing across sessions" below) *before* doing
   anything else. Write it atomically (temp file + rename, per step 2) —
   never truncate the claimed file in place. Carry the same `owner`
   (pid/sessionId) forward from the placeholder and refresh `heartbeat` to
   now, per "Owner lease" in step 2. That write is what makes the run
   recoverable if this very session ends a moment later.
5. **Present the result, then stop.** The workflow returns confirmed bugs
   (each with a worktree path, branch, reproducer, and the impact/risk/
   complexity writeup) plus a short list of what was discarded and why
   (dedup, unreproduced, low-confidence). Summarize this for the user, and
   mark the manifest `"completed"` (or delete it) now that there's nothing
   left to resume — the same atomic temp-file-plus-rename write as step 4,
   not a truncate-in-place. **Do not start fixing, committing beyond the
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
  belongs to *which* module unless you write it down, and two sessions
  starting the same module at once must not stomp each other's manifest —
  that's why step 2 claims `runs/<key>.json` with an exclusive,
  fail-if-exists write before anything else happens. The running task and
  its `runId` live in this conversation and in `/workflows` — both gone if
  the session ends. So immediately after launching (step 4 above),
  overwrite the manifest you claimed at `runs/<key>.json` (`<key>` is the
  sanitized filename from step 2, not the raw module name) with the full
  contents, written atomically (temp file + rename, per step 2 — never a
  truncate-in-place, which could leave invalid JSON and lose the `runId`
  if interrupted mid-write):

  ```json
  {
    "module": "alternator",
    "scriptPath": ".claude/skills/scylla-bug-hunt/workflow.js",
    "args": { "module": "alternator", "maxReproduce": 3 },
    "runId": "wf_...",
    "taskId": "...",
    "status": "running",
    "started": "<UTC timestamp>",
    "owner": {
      "pid": 12345,
      "sessionId": "<random id, e.g. $RANDOM-$$-<epoch seconds>>",
      "heartbeat": "<UTC timestamp, refreshed on every write to this manifest>"
    }
  }
  ```

  This is disposable run bookkeeping, not project knowledge — it belongs
  in `runs/` on disk (gitignored, see `runs/.gitignore`), never in the
  auto-memory system. A brand-new session with zero memory of this
  conversation can still find it by module name, pass its `runId` to
  `Workflow`, and pick up exactly where things stopped. Update its
  `status` to `"completed"` when you present the final result (step 5) so
  a later run on the same module doesn't get mistaken for a resume. Only
  the session that claimed the key in step 2 should ever write to this
  file — a session that lost the claim race must not touch it.

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
