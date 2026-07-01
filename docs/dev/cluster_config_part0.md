# Cluster Config — Part 0: Shared Foundation

This document is the shared foundation for the cluster-config design. It defines the model and mechanisms common to every option category. The three category documents build on it:

- Cluster Config — Part 1: Native Cluster Config Options
- Cluster Config — Part 2: Options Migrated From `db::config.cc`
- Cluster Config — Part 3: Options Migrated From Legacy Schema Properties

The proposed initial implementation targets Part 0 and Part 1.

Read this document first, then read the category document for the option you are working with.

The target audience is Scylla developers implementing the feature and operators reasoning about the model.

## Overview

Cluster config is a centralized, feature-gated, schema-backed metadata system for a bounded set of supported configuration keys. It lets operators store overrides at multiple scopes, resolve an effective value through a fixed precedence order, inspect effective values through `DESCRIBE`, and apply supported options to a running node without a restart through registered callbacks.

The easiest way to think about it is:

- one logical override model for every scope
- one resolver that picks the effective value from the scope chain
- one schema-backed persistence path for all writes


Registry metadata lives in `db/cluster_config_registry.{hh,cc}` and is the source of truth for which keys exist, which scopes they support, what types they accept, and which registry epoch they require. Resolution and callback dispatch center on `db::cluster_config_manager`, which loads from schema-backed metadata.

Every option, regardless of category, uses the same `configs`-based stored-override model and the same scope-resolution model. The categories differ only in their CQL spelling and how (or whether) the resolved value is applied to a running node. The schema-version, digest, and prepared-statement consequences of a write are uniform across options (see Schema-Backed Ownership).

## Fundamental Model

### Scope Hierarchy

The feature defines six scopes, organized into two independent resolution domains that share `CLUSTER` as a common fallback:

- table-oriented domain: `TABLE -> KEYSPACE -> CLUSTER`
- node-oriented domain: `NODE -> RACK -> DATACENTER -> CLUSTER`

Each chain is listed from most specific to least specific. A lookup follows the chain for the domain implied by its target — not a single global ordering — and the first matching stored override along that chain wins. `CLUSTER` is the shared final fallback for both domains.

Throughout the design, "domain-specific scopes" means `KEYSPACE`/`TABLE` for the table-oriented domain and `DATACENTER`/`RACK`/`NODE` for the node-oriented domain; `CLUSTER` belongs to neither and is only the shared fallback.

### Stored Overrides and Effective Values

Each scope stores only overrides. Effective values are derived at read time by resolution across the scope chain.

The stored-override rule is simple:

- if a scope stores an override for a key, that key appears in the scope's `configs` map
- if a scope does not store an override for a key, the key is absent from the scope's `configs` map
- `= NULL` removes the stored override at the current scope by removing the key from that scope's `configs` map

The user-facing read rule is different:

- `DESCRIBE KEYSPACE` and `DESCRIBE TABLE` show effective values, not just stored overrides
- the effective value is always emitted as the real, executable property value, so replaying the output reproduces the same effective behavior
- a trailing CQL comment annotates the value's provenance as the full stored-override chain: `effective from <scope>, stored=[<scope>=<value|NULL>, ...]`, listing every scope in the option's domain chain from most specific to least specific, unconditionally; each entry shows the stored override at that single scope or `NULL` when none is stored there, and `<scope>` after `effective from` names the most-specific scope that has a stored value (or `default` when every scope is `NULL`)
- because the effective value is emitted as the actual value, a round-trip preserves behavior exactly; the comment is informational only and does not change what is replayed
- `DESCRIBE` visibility is a per-option policy decision, independent of a write's schema-version, digest, and prepared-statement consequences (see Schema-Backed Ownership)

### Schema-Backed Ownership

Cluster config treats configuration metadata as schema-owned metadata, not as a side channel.

That means:

- all scopes persist through schema-backed metadata
- all config mutations use the schema-backed mutation and agreement path
- because every write goes through that path, every config write — regardless of option or scope — bumps the global schema version and changes the schema digest
- prepared-statement invalidation is determined by scope, not by option: a write invalidates prepared statements for a table only when the write path raises a per-table schema-change notification for that table — for a native option that happens only at `TABLE` scope, and only for the targeted table; `KEYSPACE`, `CLUSTER`, `DATACENTER`, `RACK`, and `NODE` writes raise no per-table notification and invalidate no prepared statements

These consequences are uniform across options. There is no per-option "does this bump the version/digest" dial: schema-backed persistence implies version and digest participation for every write, because the schema path is also the agreement and versioning mechanism. The only consequence that varies is prepared-statement invalidation, and it varies by scope (whether the write path raises a per-table schema-change notification for the target table), not by option policy. The stored value lives out-of-band in the config tables, so even a `TABLE`-scope write does not change the target table's own per-table schema version; the invalidation comes from an explicit per-table notification raised for that one table.

This is the authoritative statement of those consequences; later sections refer back here rather than restating them.

## External Surfaces

### Management CQL Surface

Grammar lives in `cql3/Cql.g`. Statement execution will reuse existing alter-keyspace / alter-table paths where appropriate and add new node-oriented-scope statement support.

The management surface has two shapes:

- table-oriented scopes use `ALTER KEYSPACE|TABLE <target> WITH ...`
- node-oriented scopes use `ALTER DATACENTER|RACK|NODE <target> WITH ...`

`ALTER CLUSTER ... WITH ...` sets the shared `CLUSTER` fallback. Because `CLUSTER` is the final fallback for both domains, it is valid for a table-oriented or a node-oriented option.

For example, a table-oriented option is set through the table-oriented forms, with `CLUSTER` as the shared fallback:

```sql
ALTER CLUSTER WITH some_table_option = 1;
ALTER KEYSPACE ks WITH some_table_option = 2;
ALTER TABLE ks.tbl WITH some_table_option = 3;
```

A node-oriented option is set through the node-oriented forms (the datacenter, rack, and node targets are validated against the live topology), again with `CLUSTER` as the shared fallback:

```sql
ALTER CLUSTER WITH some_node_option = 7;
ALTER DATACENTER dc1 WITH some_node_option = 8;
ALTER RACK dc1 rack1 WITH some_node_option = 9;
ALTER NODE 7f4d8b1e-1c2a-4f3b-9e6d-0a1b2c3d4e5f WITH some_node_option = 10;
```

Values are not limited to scalars. An option whose value is a map reuses the CQL map spelling, e.g.,

```sql
ALTER CLUSTER WITH some_map_option = {'mode': 'repair', 'propagation_delay_in_seconds': '3600'};
ALTER KEYSPACE ks WITH some_map_option = {'mode': 'timeout', 'propagation_delay_in_seconds': '86400'};
ALTER TABLE ks.tbl WITH some_map_option = {'mode': 'disabled'};
```

Across both shapes, the user model is the same:

- setting a value creates or replaces the stored override at that scope
- if the value of the config option is a map, the override is atomic: the whole map at that scope becomes effective (missing keys are not inherited from broader scopes)
- setting `= NULL` removes the stored override at that scope
- if no stored override remains at the target scope, the value falls back to inheritance

For example, removing the table-scope override makes `ks.tbl` fall back to the keyspace value (or, if none, the cluster value, and finally the option's built-in default):

```sql
ALTER TABLE ks.tbl WITH some_table_option = NULL;
```

#### Common Validation Rules

- the feature is validated against cluster feature gating and registry version visibility
- unknown keys are rejected before any schema mutation is generated
- known keys are validated against allowed scope and declared value type before write
- `= NULL` means remove the stored override at the addressed scope and return to inheritance
- scope identifiers must refer to existing cluster, datacenter, rack, node, keyspace, and table targets; non-existing targets are rejected
- topology-target existence is checked against the authoritative topology metadata maintained by the topology subsystem; the schema tables store config overrides, not the source of truth for whether a target exists

### Introspection CQL Surface

Rather than adding a dedicated query surface for reading effective config, cluster config reuses `DESCRIBE` as the primary way to inspect table-oriented options: their effective values appear directly in `DESCRIBE KEYSPACE` and `DESCRIBE TABLE` output, alongside the other table properties.

The required behavior is:

- `DESCRIBE KEYSPACE` and `DESCRIBE TABLE` show effective values for inheritable table-oriented config, formatted as described in [Stored Overrides and Effective Values](#stored-overrides-and-effective-values) (executable value plus the provenance comment)
- `DESCRIBE CLUSTER`, `DESCRIBE DATACENTER`, `DESCRIBE RACK`, and `DESCRIBE NODE` are ignored in the first iteration; node-oriented-scope values are not introspectable through `DESCRIBE` yet

`DESCRIBE` emits the effective value as the real property value and also emits provenance as a comment.

How backup config data is restored is a backup/restore-tool policy decision. A tool may restore explicit per-target values (materialized effective state), or restore inheritance by setting broader-scope defaults plus selected lower-scope overrides. This feature is policy-neutral and provides enough information for either approach; the provenance comment is informational context and is not replayed by CQL.

`DESCRIBE TABLE ks.tbl` can produce any of:

```
... AND some_table_option = 300000;
-- effective from table, stored=[table=300000, keyspace=432000, cluster=864000]

... AND some_table_option = 432000;
-- effective from keyspace, stored=[table=NULL, keyspace=432000, cluster=864000]

... AND some_table_option = 864000;
-- effective from cluster, stored=[table=NULL, keyspace=NULL, cluster=864000]

... AND some_table_option = 864000;
-- effective from default, stored=[table=NULL, keyspace=NULL, cluster=NULL]
```

`DESCRIBE KEYSPACE ks` describes the shorter `keyspace -> cluster` chain, so it omits the `table` entry:

```
... AND some_table_option = 432000;
-- effective from keyspace, stored=[keyspace=432000, cluster=864000]

... AND some_table_option = 864000;
-- effective from cluster, stored=[keyspace=NULL, cluster=864000]
```

Stored values are rendered as the raw stored text for each scope.

## Implementation Details

### Option Registry

Core component: `db::cluster_config_registry`.

The registry is the source of truth for:

- key discovery
- declared value type
- allowed scopes
- minimum registry version
- validation of textual values before persistence

An option's allowed scopes must belong to a single resolution domain: a scope mask may combine `CLUSTER` with the table-oriented scopes (`KEYSPACE`, `TABLE`) or with the node-oriented scopes (`DATACENTER`, `RACK`, `NODE`), but never mix the two. A mixed-domain mask is a registration error, rejected when the registry is built, so every option resolves through exactly one of the two precedence chains with no ambiguity about which one wins.

### Registry Versioning

The registry version controls which options are visible, gated by cluster features rather than an operator-set value.

The minimum implementation supports a single coarse-grained registry epoch:

- existing options belong to registry epoch `v0`
- the minimum implementation is gated by `ALTER_CLUSTER_CONFIG_REGISTRY_V0`
- the current registry version is derived from enabled cluster features, not from an operator-set value

Each later epoch is introduced when a new batch of options is added: the new options are declared with `min_version` set to that epoch (`v1`, `v2`, ...) and gated by a matching cluster feature (`ALTER_CLUSTER_CONFIG_REGISTRY_V1`, `ALTER_CLUSTER_CONFIG_REGISTRY_V2`, ...). The current registry version is the highest epoch whose feature is enabled cluster-wide, so an option only becomes visible once every node supports its epoch. This keeps option visibility tied to feature gating during upgrades, without an operator-set version. The minimum implementation defines only `v0`; the concrete `v1` and later rollouts are deferred until those option batches land.

### Value Encoding And Type Validation

Config values are stored and transported as text, regardless of an option's declared type. The registry type is the single point of validation: before any mutation is generated, the textual value is validated against it (`text`, `uint32`, `floating_point`, `boolean`), and an invalid value is rejected before any schema mutation.

For an option whose value cannot be expressed as a registry type, the option may instead declare a per-option parser and validator.

### Storage Model

Cluster config uses one logical storage model across all scopes, even though the physical rows live in more than one schema table.

The logical rule is simple:

- every scope stores only overrides
- all option overrides for a single target are stored together in one `configs` map, keyed by option name
- removing a key from `configs` is the persisted form of `... WITH <name> = NULL`
- effective values are resolved later by the scope waterfall, not pre-materialized everywhere

Physically, each scope's `configs` map is stored in a different schema table:

- `system_schema.scylla_keyspaces.configs` stores keyspace overrides
- `system_schema.scylla_tables.configs` stores table overrides
- `system_schema.scylla_clusters.configs` stores cluster overrides
- `system_schema.scylla_datacenters.configs` stores datacenter overrides
- `system_schema.scylla_racks.configs` stores rack overrides
- `system_schema.scylla_nodes.configs` stores node overrides

For node-oriented scopes, each row identifies one scope target and stores a `configs` map of overrides for that target.

### Schema Mutation Path

All config mutations go through schema mutation machinery, for every scope.

That implies:

- group0 serialization with other schema changes
- schema agreement behavior rather than a side metadata agreement path
- authoritative persistence for all scopes through the same schema-owned mutation path
- existing client-visible `SCHEMA_CHANGE` events for keyspace/table mutations, but no new node-oriented-scope protocol event shape in the minimum implementation
- schema version and digest participation for every config write, because every write uses this path

The schema-version, digest, and prepared-statement consequences of using this path are stated in [Schema-Backed Ownership](#schema-backed-ownership).

### Cluster Config Manager

Core component: `db::cluster_config_manager`.

Responsibilities:

- load stored overrides from schema-backed metadata for all scopes
- resolve effective values by scope precedence
- provide a readiness barrier for users that require authoritative cache contents
- invoke each option's registered callback whenever its effective value changes
- expose lookup helpers for resolving an option's effective value
- monitor topology changes and automatically drop overrides whose node-oriented target no longer exists

The manager has one load model over schema-owned metadata.

Authoritative-load rule:

- after group0 state and topology state are authoritative, load schema-backed cluster-config metadata for all scopes
- the first callback pass must complete before the manager opens its readiness barrier
- only after that load and first callback pass complete should the manager open its readiness barrier
- consumers that require authoritative config must wait on that barrier rather than reading partially initialized state during startup, join, or restart

### Orphaned Override Cleanup

Node-oriented overrides (`NODE`, `RACK`, `DATACENTER`) are addressed to a topology target. Topology-target existence is validated at write time, but a target can disappear later through node replacement or removal, or datacenter decommission, while overrides for it still exist.

The proposed manager behavior is to handle this automatically:

- it monitors topology changes
- when a node-oriented target ceases to exist, the manager drops the overrides addressed to that target
- the cleanup goes through the same schema-backed mutation path as any other config change, so it converges consistently across the cluster

Operators do not need to manually remove overrides for decommissioned targets, and resolution never has to account for overrides addressed to a non-existent target.

### Resolution Precedence

`resolve_config()` resolves through the domain chains from [Scope Hierarchy](#scope-hierarchy) — `table -> keyspace -> cluster` for a table target, `node -> rack -> datacenter -> cluster` for a node target. Since each option's scopes are constrained to one domain at registration (see [Option Registry](#option-registry)), every key resolves through exactly one chain, with `cluster` as the shared final fallback.

If no scope has a stored override, lookup returns absence and the caller falls back to the option's built-in or subsystem default.

### Live Config Application

A consuming subsystem reads an option's effective value in one of two ways:

- **poll**: read the value on demand through the manager lookup helpers (`resolve_config`, or the per-scope `get_*_config` getters). This fits options whose consumer only needs the current value when it acts, and needs no registration.
- **callback**: register a callback once, and the manager invokes it whenever the effective value changes. This fits options whose consumer must act the moment the value changes — for example to apply it to a running node without a restart.

The rest of this section describes the callback path.

A subsystem registers a callback for an option, keyed by config name, with the manager (`register_config_callback`). Registration returns a shard-local RAII handle; destroying the handle unregisters that callback on the shard where it was registered. Callbacks work for options in **either** resolution domain — node-oriented and table-oriented alike:

```cpp
future<config_callback_registration> register_config_callback(
        sstring config_name,
  std::function<future<>(const lookup_context&, std::optional<sstring>)> on_change);
```

When an option's value changes, the manager refreshes cached values on all shards, resolves it for each affected target, and invokes registered callbacks with the target context and the resolved value. The returned future resolves after the manager has registered the callback and invoked it once with the current values, so the consumer does not need a separate initial apply step. `std::nullopt` means no scope has an effective value, so the consumer should clear its cluster-config source and restore its own default. The `lookup_context` identifies which target each call applies to. How many targets a refresh touches depends on the option's domain:

- a **node-oriented** option has a single target, the local node, so the manager resolves once and calls `on_change(node_ctx, value)`
- a **table-oriented** option targets a single table, so the manager resolves per affected table and calls `on_change(table_ctx, value)` for each.

For a given registration, the manager serializes callback invocations and waits for the returned `future<>` before invoking that registration again. The callback must still be idempotent: the manager may re-emit the same effective value after registration, restart, or a coalesced refresh. The manager only resolves the value and fans the call out. The callback owns everything behavior-specific — where the value lands and what the restore-on-unset baseline is; the manager does not snapshot anything on the subsystem's behalf.

A native option registers a callback when its consumer must react to changes (see Part 1).

Join and restart follow the same sequencing: receive authoritative topology/schema state first, refresh the manager from schema-backed metadata, then re-invoke the callbacks.

### Permissions and Authorization

The permission check is per-scope: the required authority comes from the target scope of the statement, not from the option key.

The rules are:

- table-oriented scopes use the existing `ALTER` permission model:
  - `ALTER TABLE ... WITH ...` requires `ALTER` permission on the table
  - `ALTER KEYSPACE ... WITH ...` requires `ALTER` permission on the keyspace
- node-oriented scopes are superuser-only in the minimum implementation:
  - `ALTER CLUSTER ... WITH ...`
  - `ALTER DATACENTER ... WITH ...`
  - `ALTER RACK ... WITH ...`
  - `ALTER NODE ... WITH ...`

Because the check is on the target scope, the same key can carry different permissions at different scopes: a key set at table scope requires only `ALTER` on the table, but at cluster scope requires superuser.

Finer-grained authorization for the node-oriented scopes can be designed later, but it is out of scope for the minimum implementation.

## Tests

### Unit and Boost Tests

Primary areas common to all categories:

- scope caches and waterfall resolution
- schema-backed load/refresh behavior for all scopes
- registry validation and scope rejection
- schema-version/digest and prepared-statement-invalidation behavior across scopes

Main files:

- `test/boost/cluster_config_manager_test.cc` — registry, resolution, and manager behavior
- `test/boost/schema_change_test.cc` — schema-version/digest effects of config writes
- `test/boost/cql_query_test.cc` — `ALTER ... WITH ...` CQL surface, including the node-oriented path via a test-only injected option

### Cluster Tests

Primary areas common to all categories:

- end-to-end `ALTER ... WITH ...` behavior across scopes
- `= NULL` override removal across scopes
- joining-node behavior with pre-existing schema-backed config metadata

Main file:

- `test/cluster/test_cluster_config.py`

Each category document lists the additional, category-specific test areas.
