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

Where tablets are the default (`tablets_mode_for_new_keyspaces`), the keyspace is created on tablets and its replication is managed by the automatic replication factor reconciler with a goal of two racks per DC (see below); otherwise it is created as before.

## `system_audit`/`audit`

Internal audit-logging keyspace used to persist audit events when table-backed auditing is enabled.

Scylla's audit table storage is implemented as an internal audit keyspace for audit records (for example, auth/admin/DCL activity depending on audit configuration). In current code this keyspace is named `audit`, while operational material may refer to it as its historical name (`system_audit`). It is intended for security/compliance observability, not for application data.

Where tablets are the default, the keyspace is created on tablets and its replication is managed by the automatic replication factor reconciler with a goal of three racks per DC (see below); otherwise it is created on vnodes with `NetworkTopologyStrategy` and RF 3 in every DC of the topology, as before (a DC of zero-token nodes only included).

### Automatic replication factor of `audit` and `system_traces`

Where tablets are the default and the `AUTO_REPLICATION_FACTOR` cluster feature is enabled when the keyspace is created, both keyspaces are created by `table_helper::setup_auto_rf_keyspace()` with `NetworkTopologyStrategy` and RF 1 in every DC which has a normal, non-draining token-owning node; a DC of zero-token nodes only, such as an arbiter DC, is left out, since a tablets keyspace cannot place a replica there. The RF is a one-rack rack list where `rf_rack_valid_keyspaces` or `enforce_rack_list` is on and the `RACK_LIST_RF` feature is enabled, numeric otherwise. A keyspace created before `AUTO_REPLICATION_FACTOR` is enabled (during a rolling upgrade) is created on vnodes and stays there; the reconciler only manages tablets keyspaces.

From then on the topology coordinator reconciles their replication (`find_auto_rf_change()` and `next_auto_rf_change()` in `service/topology_coordinator.cc`) against two sets of racks, computed by `get_auto_rf_racks()`. The *eligible* racks of a DC are those in which some non-auto-RF tablets keyspace places replicas (a numeric RF makes every rack of the DC eligible; an RF of 0 makes none). The *allowed* racks are the eligible racks with a token-owning, non-excluded normal node.

Each DC has a mode (`get_auto_rf_dc_mode()` in `service/topology_utils.cc`):

- *rack_list* where `rf_rack_valid_keyspaces` or `enforce_rack_list` is on (read from the coordinator's own config) or an eligible keyspace in the DC uses a rack list, and the `RACK_LIST_RF` feature is enabled: the keyspace uses a rack list following the eligible racks;
- *numeric* where the eligible keyspaces of the DC all use numeric RFs and neither option is on: the keyspace keeps a numeric RF, raised up to min(goal, largest numeric RF of those keyspaces, allowed racks) and lowered when it is above min(largest numeric RF, allowed racks) (the goal does not bound lowering, so manual raises within that are kept). A numeric RF names no rack, so it never keeps a rack from being removed which the user keyspaces could leave, and the cap keeps it from needing more racks than they do;
- *keep* where no eligible keyspace uses the DC and neither option is on: the keyspace keeps its form, shrunk to one replica.

While the options are on but `RACK_LIST_RF` is not yet enabled, the reconciler does nothing. One `keyspace_rf_change` request is issued at a time, the first of:

- numeric RFs in rack_list DCs are converted to rack lists of the same size: allowed racks, or, in a DC no eligible keyspace uses, racks with token owners preferring those holding the keyspace's replicas (this runs even while no other tablets keyspace exists); a numeric RF above the racks available is lowered first where numeric changes are allowed, and left alone otherwise;
- a numeric RF above its bound is lowered by one, or a rack which is no longer eligible is dropped, never a DC's last rack (a DC no eligible keyspace replicates to is shrunk to one replica, never removed), and nothing is lowered or dropped while no eligible keyspace exists at all (fresh cluster, last user keyspace dropped);
- a numeric RF below its target is raised by one, or an allowed rack is added while the list is below the goal (in a DC whose eligible keyspaces are all numeric, the goal is capped by their largest RF);
- an allowed DC the keyspace does not replicate to yet is added, with RF 1 in numeric mode or one rack otherwise.

Changes are deferred while a normal node is dead, unless the node is excluded (`excluded_tablet_nodes`): the barrier skips excluded nodes, and giving up a lost rack is what lets its nodes be removed. Rejected changes back off exponentially per keyspace (1 s ... 60 s). Nothing is scheduled while tablet load balancing is disabled, so `disable_tablet_balancing` also freezes auto-RF. A manual `ALTER` is undone where it contradicts the rules above (a DC removed by hand is added back while another tablets keyspace still replicates to it) and kept otherwise. Keyspaces which already exist on vnodes (clusters upgraded from a release without this feature) are never touched. `system.topology.needs_auto_rf_change` is set while a change is to be made and preempts tablet load balancing; the quiesce topology request schedules and waits out pending changes; decommission and removenode wait, for up to 120 s, before rejecting a removal as RF-rack-invalid when the reconciler is going to make it valid (`storage_service::auto_rf_may_still_fix_removal()`): only the auto-RF keyspaces make the removal invalid, the rules above say the reconciler gives the node's rack up, and it is able to act (balancing enabled, no non-excluded node down). The decision does not depend on how soon the reconciler gets to it.

Applying a schema change which touches `system_schema.keyspaces` wakes the coordinator (`group0_state_machine`), since creating or altering a keyspace is what makes racks eligible; otherwise the reconciler would only notice on the next load-stats refresh. Each change is an ordinary `keyspace_rf_change` global topology request (visible in the task manager's `global_topology_requests` module); the coordinator logs "Scheduling auto RF change for keyspace ..." at info, deferrals at info (rate limited), and rejections of its own requests at warn (rate limited) instead of error. `storage_service/quiesce_topology?auto_rf_only=true` (the `auto_rf_quiesce` global topology request) waits only for the reconciler: it runs `find_auto_rf_change()` under the group0 guard, schedules a due change, and answers once nothing is due, queued, running or backed off, without waiting for the tablet balancer; tests use it through `wait_for_auto_rf_settled()`. `storage_service/ownership` ignores the auto-RF keyspaces when deciding whether the cluster uses tablets, since they do not affect the token ring.

Test-only error injections: `auto_rf_keyspaces_use_vnodes` (create both keyspaces on vnodes), `skip_auto_rf_change` (the reconciler schedules nothing) and `keyspace_rf_change_fail` (reject every `keyspace_rf_change` of the keyspace given as parameter, to drive the backoff).

Numeric mode can leave the auto-RF keyspaces RF-rack-invalid (e.g. system_traces at RF 2 on three racks). When `rf_rack_valid_keyspaces` or `enforce_rack_list` is turned on later, the startup checks (`database::check_rf_rack_validity()`, `check_rack_list_everywhere()`, called from `main.cc` with `auto_rf_keyspaces()`) let the auto-RF keyspaces through with a warning, and the reconciler, in rack_list mode now, converts them.

Known limitation: in a DC whose user tablets keyspaces mix numeric RFs and rack lists, every rack is eligible and the auto-RF keyspaces use rack lists, which are then never given up (a rack list cannot go back to numeric). Decommissioning or removing all nodes of a rack such a keyspace lists is blocked: the RF-rack validity check rejects it, or the drain of the last node fails. The workaround is to disable tablet load balancing (which pauses the reconciler; node drains still run), drop the rack from both keyspaces by hand, remove the nodes, then re-enable balancing.
