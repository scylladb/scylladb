# Review Checklist

## How to use this document

This document describes general guidelines for reviewing patches to
Scylla. The guidelines are not absolute; with good reason they can be
overridden at the discretion of the maintainers. Nevertheless, patches
should try to conform to these guidelines instead of finding reasons for
seeking an exception.

## Code style

Code should conform to the project's coding style, see
[Seastar coding style](https://github.com/scylladb/seastar/blob/master/coding-style.md).


## Patch organization

Changes should be organized into one or more patches, depending on
how big the change is, according to the following
guidelines:
- Patches should be small.
- Patches should all individually compile and be correct
  (all tests should pass). You are not required to verify
  this but do look for any signs in the code that
  suggest this might be violated. For example a patch
  introducing a regression test being ordered before the
  fix. Or a method being used before the patch that
  introduces it.
- Patches should be self-contained and do one thing.
- Each patch should have a descriptive commit log.
  The patch subject should have to following form: "module: changes".
  The "module" is the module (usually a directory) that the changes apply
  to. Multiple ones can be listed, concatenated with ",". If a patch
  applies to the entire tree, use the "tree:" prefix.
  In certain cases, the "module: " prefix can be omitted altogether, e.g.
  when changing files in the root directory.
  The "changes" is a short summary of the changes (similar to a title).
  Avoid generic patch titles like: "sstables: fix the bug", instead make
  it specific to the patch: "sstables: close file-descriptor on error".
  Try to limit lines in the commit log to 72 chars width. This is not
  always possible, especially for the subject line.
- The patch description should not assume the reviewer is an
  expert in the matter. It should include enough context
  that a non-expert can understand, on their way to becoming
  an expert.
- The patch description should be self-contained and not
  refer to out-of-band discussions ("as agreed on the daily
  call").
- Each patch should have a motivation for the change.
  Patch descriptions that simply say "make X do Y" without
  explaining why that's a good thing are considered bad.
  A good litmus test is whether the inverse patch ("make X
  not do Y") is just as compelling as the original patch. If it
  is, why prefer one over the other?
- Each pull request should include a motivation for the
  change(s). For multi-patch series, this should be in
  the cover-letter, for single patch ones in the patch
  description.
- A series should tell a story that leads up to the change. Small
  preparatory patches come first, such as a refactor that does not
  change behavior or plumbing for a new parameter, so that the patch
  with the fix is small and obvious. Look for preparatory changes
  packed into the fix patch, and for patches that each do something
  but do not build toward the fix. A later patch can touch code from
  an earlier one, but should not rework it: a rewrite usually belongs
  in the earlier patch.

## Naming

Names should avoid abbreviations (industry-standard abbreviations
are acceptable) as they make the code opaque to newcomers. Within
a restricted scope such as a function or a class, local or private
variables can be given abbreviated names.

## Micro-performance

The following should be avoided:
 - computational loops without preemption checks and yields
 - allocation of large contiguous memory blocks in non-initialization
   code
 - quadratic (or worse) behavior on user-controlled inputs
 - destroying a large container in one go instead of with
   `utils::clear_gently()` (`utils/stall_free.hh`)
 - recursion over user-controlled nesting without a depth limit

## Concurrency

Code that generates concurrent behavior should include means to control
the amount of concurrency, placing an upper bound on the amount of memory
in use by concurrent operations.

If a component performs work in the background, it would include a mechanism
(`stop()` or `close()`) to wait for background work to complete. If background
work is unbounded in time, the code should be able to abort in-progress work.

Work should run in the scheduling group it belongs to. Take care with
work handed to a long-lived fiber or queue: a task runs in the group that
was current when it was created, so that work runs in the group of the
fiber, not in the group of the request that handed it over.

Startup and shutdown should be considered together: services should stop
in the reverse order of their dependencies, and the code should handle a
startup that aborts halfway, a restart after an interrupted one, and a
request that arrives during shutdown.

## Unit testing

Patches should be accompanied by unit test when feasible.
 - patches that fix a bug should be accompanied by a test that fails
   before the patch is applied and passes after it is applied
 - patches that introduce generic code (such as a container) should
   test all aspects of that code, not just those that are used by
   the patchset.

## Templates

In non performance sensitive code, type erasure should be preferred
to templates in order to reduce code and compile time bloat. In performance
sensitive code, template bloat can be justified.

Template parameters should be constrained by C++ concepts, both as
code documentation, and to enable early error checking by the compiler.

Templates should not be used as an ad-hoc method to reduce code duplication;
rather they should be used when a common concept can be captured in
template code and reused.

## Singletons

The singleton pattern should be avoided when it can introduce
confusion about initialization order or make unit testing difficult. Instead,
dependencies should be passed via constructor parameters (the "dependency
injection" pattern).

## Invariant checking in scylla

Non obvious invariants should be checked. There are three ways of
doing it, asserts, throwing exceptions and logging failures.

If an invariant is critical for the stability of the system, use an
assert. For example, a broken invariant in the memory allocator can
lead to data corruption and there is no way to work around it, so
assert.

If an invariant is needed only by some features and the system can
shutdown cleanly without it, then a throw is appropriate. If the issue
was found while serving a user request the exception is propagated
back to the user. It if was a scylla internal operation, it will still
bring down the node.

When the operation can just be ignored, logging the failure is the
best option. The canonical example is when an element is "known" to be
in a container, but while trying to remove it we find out that it was
not there.

## Code paths

A fix should cover every path into the changed code, not only the one in
the report. Pairs of paths that often differ:
 - prepared and unprepared statements
 - local and forwarded requests, coordinator and replica
 - tablets and vnodes
 - LWT and plain writes, strongly and eventually consistent tables
 - CQL and Alternator
 - the first page and later pages
 - top-level and nested expressions

Avoid copying shared logic, for example into a special-purpose statement
or an internal query: the copy misses later fixes to the original. Moving
both onto one helper is usually better than fixing both, and reusing
existing code is better than a parallel implementation. A new path into
existing functionality should repeat every check of the old one:
validation, guardrails, paging, limits, routing. Where it differs on
purpose, the patch description should say so.

A fix should go where the behavior first goes wrong, not only where the
symptom shows up. A check at the symptom, for example rejecting a bad
value where it crashes instead of where it enters, is welcome as an extra
layer of defense. The description should then say whether the patch fixes
the cause or only guards against the symptom, and how a real cluster
reaches the fixed case.

## Errors

A user mistake should be reported with the exception type the code already
uses for similar mistakes (for example
`exceptions::invalid_request_exception`), so the client gets the right CQL
error code. Exceptions that do not derive from
`exceptions::cassandra_exception` reach the client as SERVER_ERROR. The
message should be specific and the same on every path. Use "Not yet
supported" only where support is planned, since it promises support.

## Upgrades and mixed-version clusters

During a rolling upgrade, old and new nodes talk to each other. New
behavior that old nodes do not understand should be gated by a cluster
feature (`gms/feature_service.hh`), and the code should behave correctly
until the feature is enabled. Consider what an old node does with a new
RPC verb or field, or a new system table column, and whether the cluster
can roll back. Such a change may need an upgrade test.

Treat changes to IDL, sstables, the commitlog or system table schemas as
format changes: old nodes, and older versions after a rollback, have to
read the result. Changing the type of an IDL field is a format change too,
unless both types have the same encoding. New IDL fields should carry a
`[[version X.Y]]` attribute, see `IDL.md`.

## Existing data

A new rule should not reject what is accepted today: existing schemas and
stored values that break the rule should keep working. For nodes that
already hit a bug, the description should say whether the fix repairs them
or they need a manual step and a release note.

Validate a value before it is stored in the schema, in group0 or in the
commitlog, while a bad value can be rejected with a CQL error. A stored
value is read back at load, flush or replay, often on every node, and a
bad one there can abort the node.

## Cluster state

It should be clear where state lives: group0, a local system table,
gossip, or memory only. After a group0 change, a reader on another node
may need a `read_barrier` first. A group0 apply or snapshot load should
not do slow synchronous work.

Readers and tests that need the applied state of a raft-backed write
should wait for it: the write can return once it is committed, before
every node has applied it.

A decision based on in-memory state should use a list of what is known to
be safe, not a list of what is unsafe. Between startup steps the state can
be empty or stale, and an empty list should then mean unsafe.

Code and tests should allow for topology changes while an operation runs:
tablet migration, node join, decommission, replace. A replica set read
once can be stale when it is used, and the tablet load balancer can move a
tablet between two steps of a test.

A decision made on a snapshot and acted on later should be checked again.
State captured at the start of a request, such as the current keyspace,
the schema version or a prepared statement, can be outdated after a
suspension point, and a table or keyspace can be dropped or altered while
the operation runs.

Consider failures: a node that is down, a timeout, a network partition,
several data centers. Retries should be safe to repeat, and the client
should get a meaningful error, not SERVER_ERROR for an internal timeout.

## Configuration

Check that a new option is needed at all. New options should be cluster
configuration options, see `docs/dev/cluster_config.md`.

## Other interfaces

CQL behavior that differs from Cassandra should be deliberate and
documented. Document a ScyllaDB extension on the regular CQL page of its
statement, and mark the heading with `` :label-note:`ScyllaDB Extension` ``.
Check whether a change also affects the REST API, nodetool, virtual
tables, `DESCRIBE` output, the drivers or cqlsh.

## Optimizations

An optimization should state its gain on production-like data, its memory
cost, and how it was measured, with the baseline and the patched build run
under the same conditions.
