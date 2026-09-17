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

## Status: already fixed upstream, and already pinned on master

This is fixed in Seastar itself, not in scylladb:

- Seastar commit `d17a4f2b`, "rpc: tune `_outstanding` map to reduce rehash
  allocations" (`Fixes: SCYLLADB-1541`), sets
  `_outstanding.max_load_factor(8); _outstanding.reserve(4096);` in the
  primary `rpc::client::client(...)` constructor
  (`seastar/src/rpc/rpc.cc`, ~line 966-971). This raises the load factor and
  pre-reserves capacity so the map rarely rehashes, avoiding the large
  contiguous allocations.
- scylladb pinned that seastar commit into the superproject's `seastar`
  submodule on 2026-07-01, in commit `8ec922d6b0` ("Update seastar
  submodule"). The current submodule pin on `master`
  (`bf2bde084b`) is `ea4fa1122c624b3e1b7e983de09c6020f91352d0`, which
  includes `d17a4f2b`.

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
Changing submodule content/pointer is handled via
`scripts/refresh-submodules.sh` and the submodule-update PR process, not via
a source change in this repository — and no submodule update is needed here
since the fix is already pinned.

## Recommendation

- **master / trunk**: close SCYLLADB-2533 as already fixed (submodule pin
  since 2026-07-01, `8ec922d6b0`), or mark as a duplicate of SCYLLADB-1541.
- **2026.1.x / 2026.2.x**: recurrences are expected — backporting the seastar
  fix to these branches was explicitly declined, tracked separately as
  SCYLLADB-3023 ("Won't Fix").
- **Open item for the ticket owner**: a few Jira comments report the same
  167936/339968-byte signature on `2026.4.0~dev`/scylla-master builds as
  recently as 2026-09-05, which postdates the pin. This could not be
  verified against those specific builds' actual seastar submodule pin from
  this environment — worth double-checking whether those builds predate the
  pin (stale build) or whether a distinct code path is producing the same
  allocation sizes by coincidence.
