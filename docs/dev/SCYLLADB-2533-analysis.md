# SCYLLADB-2533: RPC client oversized allocation warning — RCA

## Summary

Under heavy speculative digest-read load, `seastar::rpc::client` logs seastar's
"oversized allocation" WARNING with sizes of exactly 167936 and 339968 bytes.
The allocations come from the `rpc::client::_outstanding` hashtable (the map
of in-flight RPC IDs to pending-reply state), not from any scylla-side code.

## Root cause

`_outstanding` is a `std::unordered_map`. libstdc++'s default max load factor
(1.0) combined with its rehash growth policy means that once the map crosses
a bucket-count threshold, it allocates a new contiguous bucket array sized to
the next prime bucket count. Under speculative-read bursts the map grows
through several such rehashes, and the resulting bucket-array allocations
happen to page-round to exactly 167936 and then 339968 bytes — well past
seastar's oversized-allocation threshold — triggering the WARNING even though
nothing is actually leaking or misbehaving.

## Status: fixed upstream, pinned on master only

This is fixed in Seastar itself, not in scylladb:

- Seastar commit `d17a4f2b` ("rpc: tune `_outstanding` map to reduce rehash
  allocations", `Fixes: SCYLLADB-1541`) sets
  `_outstanding.max_load_factor(8); _outstanding.reserve(4096);` in the
  primary `rpc::client::client(...)` constructor
  (`seastar/src/rpc/rpc.cc`, ~line 966-971). This raises the load factor and
  pre-reserves capacity so the map rarely rehashes, avoiding the large
  contiguous allocations.
- scylladb PR #30570 ("Update seastar submodule", merged 2026-07-01, merge
  commit `010c5c565e`) pinned this fix into `master`'s `seastar` submodule.
  That PR actually bundled two unrelated seastar fixes: SCYLLADB-2937 (a
  crash fix, "reactor: don't backtrace during seastar thread switches") and
  SCYLLADB-1541 (this `_outstanding` tuning fix, `d17a4f2b`). The current
  submodule pin on `master` (`ea4fa1122c`, as of 2026-09-14) includes
  `d17a4f2b`.

## Backport status: confirmed NOT backported to 2026.1.x / 2026.2.x

PR #30570's own description states its backport policy explicitly:
"SCYLLADB-2937 is not a regression, but as it causes a crash it needs to be
backported to all live versions. SCYLLADB-1541 is a minor improvement and
does not merit a backport." So the PR's `backport/2025.1`, `backport/2026.1`,
and `backport/2026.2` labels apply only to the SCYLLADB-2937 crash fix, not
to the SCYLLADB-1541 rpc map fix.

This was verified two independent ways:

- **Git ancestry**: seastar commit `d17a4f2b` is not an ancestor of the
  seastar submodule pin on `branch-2026.1` (`bf882627c3`, as of 2026-09-08)
  or `branch-2026.2` (`53fa423920`, as of 2026-09-08). The actual
  submodule-bump commits that reached both branches on 2026-07-02 ("Update
  seastar submodule (crash on aarch64 on stall report during context
  switch)") cherry-picked only the single SCYLLADB-2937 commit (seastar
  range `94a5b8c296..53aa2180a2`, one commit). No later submodule bump on
  either branch (checked through the 2026-09-08 "RPC stream connection
  closing fix" bump) picked up `d17a4f2b`.
- **Jira**: SCYLLADB-3023 ("[Backport 2026.1] - Update seastar submodule",
  sub-task of SCYLLADB-1541) is "Won't Fix" / "Won't Do". SCYLLADB-3021
  ("[Backport 2026.2] - Update seastar submodule", same parent) is marked
  "Done", but its only comment (Avi Kivity) reads "We're not backporting the
  fix for [SCYLLADB-1541]." — i.e. "Done" here means the decision to decline
  the backport was finalized, not that the fix shipped. This matches the git
  finding above.

Recurrences of this WARNING on 2026.1.12/2026.1.13/2026.2.6/2026.2.7 builds
are therefore expected and consistent with a deliberate decision, not a new
bug or a failed backport.

## Why no code change is included here

There is no scylla-side lever to pull even if a superproject-side fix were
wanted:

- `rpc::client::_outstanding` is a private member of `seastar::rpc::client`;
  `rpc::client_options` (the public knob scylla passes in) exposes no
  capacity/load-factor setting.
- scylla's `rpc_protocol_client_wrapper`
  (`message/rpc_protocol_impl.hh`) *composes* an `rpc::client` via
  `std::unique_ptr`, it does not subclass it, so there is no override point
  from scylla's side either.

The actual fix lives in, and is already pinned from, the `seastar` submodule.
Changing the submodule pin is handled via `scripts/refresh-submodules.sh` and
the submodule-update PR process, not via a source change in this repository —
and no submodule update is needed here since master already carries the fix.

## Recommendation

- **master / trunk**: close SCYLLADB-2533 as already fixed (submodule pin
  since 2026-07-01, PR #30570), or mark as a duplicate of SCYLLADB-1541.
- **2026.1.x / 2026.2.x**: recurrences are expected and by design — the
  backport was explicitly declined (SCYLLADB-3023 "Won't Fix",
  SCYLLADB-3021 "Done" meaning the decline was finalized). No further action
  needed on these branches unless the backport decision is revisited.

## Open follow-up (unresolved — needs node-version verification)

A small number of reports carry the identical 167936-byte signature on
scylla-master/`2026.4.0~dev` builds dated *after* 2026-07-01, i.e. after
master's seastar pin should already carry the fix — e.g. runs `aa99d7ad`
(2026-08-15) and `1312448b` (2026-09-05). This is **not** resolved by the
backport cross-check above and should not be guessed at here. Two
possibilities to check against the actual run data:

1. These are rolling-upgrade tests running mixed versions mid-upgrade, and
   the affected node was still running the pre-upgrade (old) binary at the
   moment of the event, so it predates the fix despite the run's nominal
   date/version.
2. The fix's parameters (`max_load_factor(8)`, `reserve(4096)`) are
   insufficient under very high per-connection concurrency, and a distinct,
   still-unfixed rehash threshold is being hit even on binaries that carry
   `d17a4f2b`.

Flagging this as an explicit open item for whoever picks up SCYLLADB-2533
next: confirm the actual binary/seastar-pin running on the affected node at
the time of each of these two events before drawing any conclusion.
