# System Keyspaces Overview

This page gives a high-level overview of several internal keyspaces and what they are used for.

## Table of Contents

- [system_replicated_keys](#system_replicated_keys)
- [system_distributed](#system_distributed)
- [system_distributed_everywhere](#system_distributed_everywhere)
- [system_auth](#system_auth)
- [system](#system)
- [system_schema](#system_schema)
- [system_traces](#system_traces)
- [system_audit/audit](#system_auditaudit)

## `system_replicated_keys`

Internal keyspace for encryption-at-rest key material used by the replicated key provider. It stores encrypted data keys so nodes can retrieve the correct key IDs when reading encrypted data.

This keyspace is created as an internal system keyspace and uses `EverywhereStrategy` so key metadata is available on every node. It is not intended for user data.

## `system_distributed`

Internal distributed metadata keyspace used for cluster-wide coordination data that is shared across nodes.

In practice, it is used for metadata such as:

- materialized view build coordination state
- CDC stream/timestamp metadata exposed to clients
- service level definitions used by workload prioritization

This keyspace is managed by Scylla and is not intended for application tables.
It is created as an internal keyspace (historically with `SimpleStrategy` and RF=3 by default).

## `system_distributed_everywhere`

Legacy keyspace. It is no longer used.

## `system_auth`

Legacy auth keyspace name kept primarily for compatibility.

Auth tables have moved to the `system` keyspace (`roles`, `role_members`, `role_permissions`, and related auth state). `system_auth` may still exist for compatibility with legacy tooling/queries, but it is no longer where current auth state is primarily stored.

## `system`

This keyspace is local one, so each node has its own, independent content for tables in this keyspace. For some tables, the content is coordinated at a higher level (RAFT), but not via the traditional replication systems (storage proxy).

See the detailed table-level documentation here: [system_keyspace](system_keyspace.md)

## `system_schema`

This keyspace is local one, so each node has its own, independent content for tables in this keyspace. All tables in this keyspace are coordinated via the schema replication system.

See the detailed table-level documentation here: [system_schema_keyspace](system_schema_keyspace.md)

## `system_traces`

Internal tracing keyspace used for query tracing and slow-query logging records (`sessions`, `events`, and related index/log tables).

This keyspace is written by Scylla's tracing subsystem for diagnostics and observability. It is operational metadata, not user application data (historically created with `SimpleStrategy` and RF=2).

Where rack lists are enforced it is created on tablets and its replication is managed automatically, with a goal of two racks per DC (see below).

## `system_audit`/`audit`

Internal audit-logging keyspace used to persist audit events when table-backed auditing is enabled.

Scylla's audit table storage is implemented as an internal audit keyspace for audit records (for example, auth/admin/DCL activity depending on audit configuration). In current code this keyspace is named `audit`, while operational material may refer to it as its historical name (`system_audit`). It is intended for security/compliance observability, not for application data.

Where rack lists are enforced it is created on tablets and its replication is managed automatically, with a goal of three racks per DC (see below); otherwise on vnodes with `NetworkTopologyStrategy` and RF 3 in every DC.

### Automatic replication factor of `audit` and `system_traces`

`table_helper::setup_auto_rf_keyspace()` creates the two keyspaces on tablets when `enforce_rack_list` is set, tablets are the default for new keyspaces (`tablets_mode_for_new_keyspaces`) and the `AUTO_REPLICATION_FACTOR`, `RACK_LIST_RF` and `TABLETS` cluster features are enabled. They start with `NetworkTopologyStrategy` and one rack in every DC which has a normal, non-draining token-owning node (a DC of zero-token nodes cannot hold a tablet replica). Otherwise they are created on vnodes with a fixed replication factor, as before, and stay there.

The topology coordinator then reconciles their replication (`find_auto_rf_change()` and `next_auto_rf_change()` in `service/topology_coordinator.cc`). It runs only where `enforce_rack_list` is set on the coordinator, and manages rack lists only: with the option every tablets keyspace names its racks, so there is something exact to follow and no numeric replication factor to convert. `get_auto_rf_racks()` computes the racks it works with:

- *eligible*: the racks some other tablets keyspace lists;
- *allowed*: the eligible racks which have a token-owning, non-excluded normal node.

One `keyspace_rf_change` request is issued at a time, the first of:

- a rack which is no longer eligible is dropped (one without a usable node first), but never a DC's last rack (a DC nothing else replicates to is shrunk to one rack, not removed), and nothing while no other tablets keyspace exists at all;
- an allowed rack is added while the rack list is below the goal;
- an allowed DC the keyspace does not replicate to yet is added, with one rack.

A manual `ALTER` is undone where it contradicts these rules and kept otherwise.

The reconciler:

- runs when the coordinator is otherwise idle, and from the quiesce topology request, which schedules a due change and waits for it (but not for a change in backoff);
- defers changes while a normal node is dead and not excluded: an RF change's barriers cannot pass such a node, while giving up the rack of an excluded node is what lets it be removed;
- schedules nothing while tablet load balancing is disabled;
- backs off per keyspace (1 s ... 60 s) after one of its own requests is rejected or fails. Its requests carry `initiating_host`, which tells them from an operator's `ALTER`; their rejections are logged at warn, not error.

Tablet load balancing yields while a change is due (`should_preempt_balancing()`). The answer is cached in the coordinator's memory and evaluated again only after an event which can change it: a keyspace created or dropped (the coordinator is a migration listener for those, since a plain schema change does not wake it), an RF change ended, a backoff expired, a new coordinator.

Test-only error injections: `auto_rf_skip_idle_path` (only the quiesce handler schedules) and `keyspace_rf_change_fail` (rejects every `keyspace_rf_change` of the keyspace given as parameter).
