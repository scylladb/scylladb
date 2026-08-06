# Cluster Config

This document is the design for cluster config. It first defines the shared foundation — the model and mechanisms common to every option category — and then covers the option categories that build on it:

- Native Cluster Config Options
- Options Migrated From `db::config.cc`
- Options Migrated From Legacy Schema Properties

The proposed initial implementation targets the shared foundation and native options; the two migrated categories are described here once they are designed.

Read the shared foundation first, then the category section for the option you are working with.

The target audience is Scylla developers implementing the feature and operators reasoning about the model.

## Overview

Cluster config is a centralized, feature-gated, schema-backed metadata system for a bounded set of supported configuration keys. It lets operators store overrides at multiple scopes, resolve an effective value through a fixed precedence order, inspect effective values through `DESCRIBE`, and apply supported options to a running node without a restart through registered callbacks.

The easiest way to think about it is:

- one logical override model for every scope
- one resolver that picks the effective value from the scope chain
- one schema-backed persistence path for all writes


Registry metadata lives in `db/cluster_config_registry.{hh,cc}` and is the source of truth for which keys exist, which scopes they support, what types they accept, and which registry epoch they require. Resolution and callback dispatch center on `db::cluster_config_manager`, which loads from schema-backed metadata.

Every option, regardless of category, uses the same stored-override model — overrides live in a `configs` map column in the schema tables (see Storage Model) — and the same scope-resolution model. The categories differ only in two things: the CQL syntax used to set and read an option at each scope, and how (or whether) the resolved value is applied to a running node. The schema-version, digest, and prepared-statement consequences of a write are uniform across options (see Schema-Backed Ownership).

## Fundamental Model

### Scope Hierarchy

The feature defines six scopes, organized into two independent resolution domains that share `CLUSTER` as a common fallback:

- table-oriented domain: `TABLE -> KEYSPACE -> CLUSTER`
- node-oriented domain: `NODE -> RACK -> DATACENTER -> CLUSTER`

Each chain is listed from most specific to least specific. A lookup follows the chain for the domain implied by its target — not a single global ordering — and the first matching stored override along that chain wins. `CLUSTER` is the last scope consulted in both domains.

`CLUSTER` is only the last *scope*, not the last resort: if no scope in the chain — including `CLUSTER` — has a stored override, the lookup returns absence and the caller falls back to the option's built-in default (the hard-coded value, or the subsystem's own default). See [Resolution Precedence](#resolution-precedence).

### Stored Overrides and Effective Values

Each scope stores only overrides. Effective values are derived at read time by resolution across the scope chain.

The stored-override rule is simple:

- if a scope stores an override for a key, that key appears in the scope's `configs` map
- if a scope does not store an override for a key, the key is absent from the scope's `configs` map
- `= NULL` removes the stored override at the current scope by removing the key from that scope's `configs` map

The user-facing read rule is different. `DESCRIBE` reports a cluster-config option as one property line per option, in ordinary WITH-clause position, in one of two forms:

- a **live property** (`AND <option> = <value>`) is the *write view*: exactly what is stored at the described object's own scope
- a **commented-out property** (`-- AND <option> = <effective>`) is the *read view*: the effective value the object inherits without storing anything itself

Both forms end with a trailing inline comment carrying the value's provenance:

```
AND <option> = <value>  -- from <scope> (<scope>=<value|NULL>, ...)
-- AND <option> = <effective>  -- from <scope> (<scope>=<value|NULL>, ...)
```

**Live property rule.** A live property is emitted if and only if the object's own `configs` map stores the option. No folding, no inheritance: emitting an inherited or defaulted value as a live property would, on replay, invent an override at that scope and detach the object from later broader-scope changes. Because every live property is exactly a stored override, replaying `DESCRIBE` output as-is reproduces the stored `configs` maps (no invented overrides, no dropped ones), so later ALTERs at broader scopes and future default changes keep propagating exactly as they did in the source cluster.

**Commented property rule.** An option that stores nothing at the described scope but resolves through a broader scope appears as the same property line, commented out, carrying the effective value. The line is real, executable CQL behind its marker: erasing just the leading `-- ` and replaying pins the currently effective value at the described scope as an explicit override. Pinning is always this deliberate one-keystroke edit, never something replay does on its own.

**Provenance trailer.** To read the trailing comment:

- the scope after `from` names where the effective value comes from
- the parenthesized chain lists every scope of the option's domain chain visible at the described scope, most specific first, unconditionally; each entry shows the override stored at exactly that scope, or `NULL` when that scope stores nothing
- the effective value on the property's right-hand side is the resolution of exactly the printed chain (the leftmost non-`NULL` entry), so the value, the `from` scope, and the chain can never disagree
- values are rendered as CQL literals (text values single-quoted, numeric and boolean values bare) on both sides, so the effective value is directly executable
- the trailer stays a valid inline comment after the property is uncommented; it is informational only, is never replayed by CQL, and is a snapshot: after an uncomment-and-replay the next `DESCRIBE` regenerates it against the new stored state

For example, `DESCRIBE TABLE` on a table that stores nothing itself, under a keyspace storing `false` and a cluster storing `true`, emits

```
    -- AND some_option = false  -- from keyspace (table=NULL, keyspace=false, cluster=true)
```

and no live `AND some_option = ...` property: the keyspace holds the most specific stored override, and the table's own scope stores nothing.

**When nothing is shown.** An option with no stored override at any scope in the chain is invisible: no line in either form. (It resolves to its built-in default, which `DESCRIBE` does not report.)

**Tier rule.** `WITH INTERNALS` is the round-trip tier, emitted by backup: pure, replayable CQL with live properties only and no comments of either kind.

Two mechanical notes:

- in the plain tier the terminating `;` moves to its own line: the CQL `COMMENT` lexer token requires a newline terminator, so a trailing comment on the statement's last line would make the emitted `create_statement` fail to parse as a single request
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

```cql
ALTER CLUSTER WITH some_table_option = 1;
ALTER KEYSPACE ks WITH some_table_option = 2;
ALTER TABLE ks.tbl WITH some_table_option = 3;
```

A node-oriented option is set through the node-oriented forms (the datacenter, rack, and node targets are validated against the live topology), again with `CLUSTER` as the shared fallback:

```cql
ALTER CLUSTER WITH some_node_option = 7;
ALTER DATACENTER dc1 WITH some_node_option = 8;
ALTER RACK dc1 rack1 WITH some_node_option = 9;
ALTER NODE 7f4d8b1e-1c2a-4f3b-9e6d-0a1b2c3d4e5f WITH some_node_option = 10;
```

Values are not limited to scalars. An option whose value is a map reuses the CQL map spelling, e.g.,

```cql
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

```cql
ALTER TABLE ks.tbl WITH some_table_option = NULL;
```

#### Common Validation Rules

- the feature is validated against cluster feature gating and registry version visibility
- unknown keys are rejected before any schema mutation is generated
- known keys are validated against allowed scope and declared value type before write
- `= NULL` means remove the stored override at the addressed scope and return to inheritance. Only the bare `NULL` keyword removes: the string literal `= 'null'` is an ordinary value and is validated like any other, so a boolean or numeric option rejects it and a text option stores it
- a text value must not contain a line break or `*/`: `DESCRIBE` echoes every effective value back inside a `-- ...` provenance comment (and, for a described CDC log or paxos table, inside a `/* ... */` block), and a CQL string literal has no escape for either, so such a value would make the dump stop being replayable
- scope identifiers must refer to existing cluster, datacenter, rack, node, keyspace, and table targets; non-existing targets are rejected
- topology-target existence is checked against the authoritative topology metadata maintained by the topology subsystem; the schema tables store config overrides, not the source of truth for whether a target exists

### Introspection CQL Surface

Rather than adding a dedicated query surface for reading effective config, cluster config reuses `DESCRIBE` as the primary way to inspect table-oriented options: their effective values appear directly in `DESCRIBE KEYSPACE` and `DESCRIBE TABLE` output, alongside the other table properties.

The required behavior is:

- `DESCRIBE KEYSPACE` and `DESCRIBE TABLE` show effective values for inheritable table-oriented config, formatted as described in [Stored Overrides and Effective Values](#stored-overrides-and-effective-values): live properties for stored overrides, commented-out properties for inherited values, each with the provenance trailer
- `DESCRIBE SCHEMA` emits the cluster-scope overrides first, before any keyspace and in **all** tiers, as a single synthetic row (`keyspace_name = null`, `type = 'cluster_config'`) whose `create_statement` holds one `ALTER CLUSTER WITH <option> = <value>;` statement per stored option, one per line, each annotated with the provenance trailer in the plain tier and bare under `WITH INTERNALS` (cluster scope is portable operator state; emitting it only under INTERNALS would make the default dump silently lossy: live statements are stored-only, so nothing else in the dump carries the cluster scope)
- the node-oriented introspection surfaces — `DESCRIBE DATACENTER`, `DESCRIBE RACK`, `DESCRIBE NODE`, a `config` column on `DESCRIBE CLUSTER`, and a topology block in `DESCRIBE SCHEMA WITH INTERNALS` — are planned as a follow-up; no shipping option supports the node-oriented scopes yet, so nothing is introspectable there today
- these read surfaces require no special privileges beyond login: the config tables live in `system_schema` and are readable by any user, so a dump never silently omits config state depending on who runs it

The per-object `DESCRIBE`s and the schema-dump cluster block all follow the two-form rule from [Stored Overrides and Effective Values](#stored-overrides-and-effective-values): the live property is the write view, the commented-out property is the read view.

Restore semantics are additive, not reconciling: replaying a dump stores exactly the overrides it contains and never emits `= NULL` for unset options. How backup config data is restored beyond that is a backup/restore-tool policy decision; the commented-out properties and provenance trailers are informational and are not replayed by CQL, and pinning an inherited value is only ever the user's deliberate uncomment edit.

`DESCRIBE TABLE ks.tbl` can produce any of (statement elided):

```
... AND some_table_option = 300000  -- from table (table=300000, keyspace=432000, cluster=864000)
;

...
    -- AND some_table_option = 432000  -- from keyspace (table=NULL, keyspace=432000, cluster=864000)
;

...
    -- AND some_table_option = 864000  -- from cluster (table=NULL, keyspace=NULL, cluster=864000)
;
```

Only the first case stores an override at table scope, so only it carries a live property; the other two say what is in force, and that nothing is stored here, through the commented-out property alone.

`DESCRIBE KEYSPACE ks` describes the shorter `keyspace -> cluster` chain, so it omits the `table` entry:

```
... AND some_table_option = 432000  -- from keyspace (keyspace=432000, cluster=864000)
;

...
    -- AND some_table_option = 864000  -- from cluster (keyspace=NULL, cluster=864000)
;
```

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
- the minimum implementation is gated by `CLUSTER_CONFIG_REGISTRY_V0`
- the current registry version is derived from enabled cluster features, not from an operator-set value

Each later epoch is introduced when a new batch of options is added: the new options are declared with `min_version` set to that epoch (`v1`, `v2`, ...) and gated by a matching cluster feature (`CLUSTER_CONFIG_REGISTRY_V1`, `CLUSTER_CONFIG_REGISTRY_V2`, ...). The current registry version is the highest epoch whose feature is enabled cluster-wide, so an option only becomes visible once every node supports its epoch. This keeps option visibility tied to feature gating during upgrades, without an operator-set version. The minimum implementation defines only `v0`; the concrete `v1` and later rollouts are deferred until those option batches land.

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

- The `configs` map in `system_schema.scylla_keyspaces` stores keyspace overrides.
- The `configs` map in `system_schema.scylla_tables` stores table overrides.
- The `configs` map in `system_schema.scylla_clusters` stores cluster overrides.
- The `configs` map in `system_schema.scylla_datacenters` stores datacenter overrides.
- The `configs` map in `system_schema.scylla_racks` stores rack overrides.
- The `configs` map in `system_schema.scylla_nodes` stores node overrides.

For node-oriented scopes, each row identifies one scope target and stores a `configs` map of overrides for that target.

#### Schema

The table-oriented scopes reuse two existing schema tables, each gaining one new column. `configs` is a non-frozen `map<text, text>` in all six tables:

```cql
-- existing table; `configs` is the only column added by this feature
CREATE TABLE system_schema.scylla_keyspaces (
    keyspace_name text PRIMARY KEY,
    ...
    configs map<text, text>
);

-- existing table; `configs` is the only column added by this feature
CREATE TABLE system_schema.scylla_tables (
    keyspace_name text,
    table_name text,
    ...
    configs map<text, text>,
    PRIMARY KEY (keyspace_name, table_name)
);
```

The remaining four scopes — `CLUSTER` plus the three node-oriented scopes — get one new table each, keyed by the scope target:

```cql
CREATE TABLE system_schema.scylla_clusters (
    cluster_name text PRIMARY KEY,
    configs map<text, text>
);

CREATE TABLE system_schema.scylla_datacenters (
    dc_name text PRIMARY KEY,
    configs map<text, text>
);

CREATE TABLE system_schema.scylla_racks (
    dc_name text,
    rack_name text,
    configs map<text, text>,
    PRIMARY KEY (dc_name, rack_name)
);

CREATE TABLE system_schema.scylla_nodes (
    host_id uuid PRIMARY KEY,
    configs map<text, text>
);
```

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

`resolve_config()` resolves through the domain chains from [Scope Hierarchy](#scope-hierarchy) — `table -> keyspace -> cluster` for a table target, `node -> rack -> datacenter -> cluster` for a node target. Since each option's scopes are constrained to one domain at registration (see [Option Registry](#option-registry)), every key resolves through exactly one chain, with `cluster` as the shared last scope.

If no scope has a stored override, lookup returns absence and the caller falls back to the option's built-in or subsystem default.

### Live Config Application

A consuming subsystem reads an option's effective value in one of two ways:

- **poll**: read the value on demand through the manager lookup helpers — normally the typed accessors (`resolve_boolean_config`, `resolve_uint32_config`, `resolve_floating_point_config`, `resolve_text_config`), which resolve the chain and return the effective value in the option's native type, applying the registered default when no scope stores an override. `resolve_config` returns `std::optional<sstring>` instead, for callers that need to distinguish "no override stored" from a value (the per-scope `get_*_config` getters are private to the manager, so every consumer goes through the precedence chain). This fits options whose consumer only needs the current value when it acts, and needs no registration.
- **callback**: register a callback once, and the manager invokes it whenever the effective value changes. This fits options whose consumer must act the moment the value changes — for example to apply it to a running node without a restart.

The rest of this section describes the callback path.

A subsystem registers a callback for an option, keyed by config name, with the manager (`register_config_callback`). Registration returns a future that resolves to a shard-local RAII handle; destroying the handle unregisters that callback on the shard where it was registered. Callbacks work for options in **either** resolution domain — node-oriented and table-oriented alike:

```cpp
future<config_callback_registration> register_config_callback(
        sstring config_name,
  std::function<future<>(const lookup_context&, std::optional<sstring>)> on_change);
```

When an option's value changes, the manager refreshes cached values on all shards, resolves it for each affected target, and invokes registered callbacks with the target context and the resolved value. Registration itself only installs/uninstalls the callback; initial application is performed by the normal refresh path. `std::nullopt` means no scope has an effective value, so the consumer should fall back to the option's registered default; passing the callback's value through the matching `cluster_config_registry::to_*` converter does that, and keeps the callback and poll paths agreeing on both the parse and the default. The `lookup_context` identifies which target each call applies to. How many targets a refresh touches depends on the option's domain:

- a **node-oriented** option has a single target, the local node, so the manager resolves once and calls `on_change(node_ctx, value)`
- a **table-oriented** option targets a single table, so the manager resolves per affected table and calls `on_change(table_ctx, value)` for each.

For a given registration, the manager invokes callbacks serially on the local shard and waits for the returned `future<>` before invoking that registration again. The manager never invokes a callback after its registration is destroyed. The callback must still be idempotent: the manager may re-emit the same effective value after registration, restart, or a coalesced refresh. The manager only resolves the value and fans the call out. The callback owns everything behavior-specific — where the value lands and what the restore-on-unset baseline is; the manager does not snapshot anything on the subsystem's behalf.

Destroying a registration while a refresh callback pass is in progress is safe: unregistration takes effect immediately for subsequent invocations and deferred cleanup guarantees iteration stability. This avoids stale callback pointers being invoked after subsystem teardown.

A native option registers a callback when its consumer must react to changes (see [Native Cluster Config Options](#native-cluster-config-options)).

Join and restart follow the same sequencing: receive authoritative topology/schema state first, refresh the manager from schema-backed metadata, then re-invoke the callbacks.

### Permissions and Authorization

The permission check is per-scope: the required authority comes from the target scope of the statement, not from the option key.

The rules are:

- table-oriented scopes use the existing `ALTER` permission model:
  - `ALTER TABLE ... WITH ...` requires `ALTER` permission on the table
  - `ALTER KEYSPACE ... WITH ...` requires `ALTER` permission on the keyspace
- `CLUSTER` and the node-oriented scopes are superuser-only in the minimum implementation:
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

Each category section lists the additional, category-specific test areas.

## Native Cluster Config Options

This section is the implementation-oriented design and developer guide for **native cluster-config options**: keys that are designed for cluster config from the start and carry no legacy surface to preserve.

The shared model — scopes, storage, resolution, schema-backed ownership, the manager, callbacks, the CQL management surface, and tests — is defined in the shared foundation above. This section covers only what is specific to native cluster-config options.

### The Native-Option Model

A native cluster-config option is designed for cluster config from the start, rather than migrated from an existing schema property or an existing `db::config.cc` entry.

The model for native cluster-config options is:

- the registry defines the canonical option name, type, supported scopes, and minimum registry version
- the CQL surface uses the registry-defined name directly rather than preserving a legacy property spelling
- stored overrides use the same `configs`-based override model
- if you do not set the value at a scope, that scope keeps no override for it
- the effective value comes from the same normal scope resolution rules used for other cluster-config keys
- node behavior and `DESCRIBE` visibility are per-option policy decisions; a write's schema-version, digest, and prepared-statement consequences are uniform (see [Schema-Backed Ownership](#schema-backed-ownership))
- if the owning subsystem registers a callback, the manager invokes it with the resolved value whenever it changes; otherwise the option remains persisted metadata read on demand

In short, native options use the cluster-config model directly: they do not inherit legacy schema-property or `db::config.cc` rules, but they share the same stored-override and scope-resolution model. Use this category whenever a key is designed for cluster config from the start with no legacy surface to preserve.

#### Default Values

The registry owns a native option's name, type, supported scopes, minimum registry version, and built-in default. The default is declared in the option's native type, as `default_value` on the registry entry:

```cpp
option{
    .name = "auto_repair_enabled",
    .type = value_type::boolean,
    .scope_mask = schema_scopes,
    .min_version = version::v0,
    .default_value = false,
}
```

Declaring it there gives every option's default a single definition that consumers read instead of hard-coding their own literal, and makes a changed default visible in a diff between two releases. `default_value` is a `std::variant` with one alternative per `value_type`, and a `static_assert` rejects an option whose declared default does not match its type, so the value a consumer reads with `std::get<T>` is always in the type `validate_value()` would have accepted for a stored override.

The default takes no part in the stored-override model: it is never written to a `configs` map and never participates in scope resolution. If no scope stores an override, `resolve_config` returns absence and the consumer falls back to the registered default. If a user explicitly stores the same value as that default, the override is still persisted in `configs`.

Consumers do not read `default_value` directly. They read an option through the registry's `to_boolean`/`to_uint32`/`to_floating_point`/`to_text` converters — or, more usually, through the manager's typed accessors that wrap them (see [Live Config Application](#live-config-application)) — so that turning a resolved value or its absence into a usable value happens in exactly one place and no call site can substitute a default of its own. A stored value that fails to parse (only reachable through a corrupt or out-of-band write, since the CQL path validates before writing) is logged and treated as absent, so a bad row degrades to the default instead of failing the read.

Because defaults live in code rather than in persisted state, an option whose default changes between releases resolves differently on upgraded and not-yet-upgraded nodes for the duration of a rolling upgrade. The mechanism is the same as changing a `db/config.cc` default, but the consequence is wider: a `db::config` default is node-local, whereas a cluster-config option is meant to resolve to one value cluster-wide, so mid-upgrade two coordinators can act on different effective values for the same target. An operator who needs a stable value across an upgrade can store it explicitly first (`ALTER CLUSTER WITH <option> = <value>`), since a stored override is persisted and outranks any default.

### External Surfaces

The shared CQL management surface and common validation rules are defined in the shared foundation above.

A native option declares which scopes it supports in the registry, and the CQL surface uses the registry-defined name directly. `auto_repair_enabled` is registered for the schema scopes (`CLUSTER`, `KEYSPACE`, `TABLE`), so it is set through the table-oriented forms, with `CLUSTER` as the shared fallback:

```cql
ALTER CLUSTER WITH auto_repair_enabled = true;
ALTER KEYSPACE ks WITH auto_repair_enabled = false;
ALTER TABLE ks.tbl WITH auto_repair_enabled = true;
```

#### Introspection Limits In The Proposed First Version

Read-back coverage depends on the option's scopes:

- a schema-scoped native option such as `auto_repair_enabled` is table-oriented: its effective value is rendered in `DESCRIBE KEYSPACE` and `DESCRIBE TABLE` (an inherited value as a commented-out property with the provenance trailer), so it has a defined read-back path from day one.
- the node-oriented introspection surfaces are planned as a follow-up (see [Introspection CQL Surface](#introspection-cql-surface)); a native option registered for the node-oriented scopes has no `DESCRIBE` read-back surface yet.

Both forms follow the rules in [Stored Overrides and Effective Values](#stored-overrides-and-effective-values). For `auto_repair_enabled`, whose chain is `table -> keyspace -> cluster`, `DESCRIBE TABLE ks.tbl` renders, for example:

```
... AND auto_repair_enabled = true  -- from table (table=true, keyspace=false, cluster=NULL)
;

...
    -- AND auto_repair_enabled = false  -- from keyspace (table=NULL, keyspace=false, cluster=true)
;

...
    -- AND auto_repair_enabled = true  -- from cluster (table=NULL, keyspace=NULL, cluster=true)
;
```

`DESCRIBE KEYSPACE ks` describes the shorter `keyspace -> cluster` chain and omits the `table` entry.

Until the node-oriented `DESCRIBE` surfaces exist, the stored overrides for node-oriented scopes are readable by querying the `system_schema.scylla_clusters`, `system_schema.scylla_datacenters`, `system_schema.scylla_racks`, and `system_schema.scylla_nodes` tables directly; effective values are readable in code through `resolve_config` and the typed `resolve_*_config` accessors (the per-scope `get_*_config` getters are private to the manager, see [Live Config Application](#live-config-application)).

### Implementation Details

The shared registry, storage model, manager, resolution precedence, and callback model are defined in the shared foundation above. The points below are specific to native options.

#### Callbacks For Native Options

A native option that needs to react to changes registers a callback with the manager — the same interface every option uses (see [Live Config Application](#live-config-application)):

```cpp
auto registration = co_await mgr.local().register_config_callback(
        "auto_repair_enabled",
        [] (const lookup_context& ctx, std::optional<sstring> value) -> future<> {
            if (value) {
                /* apply the parsed value for ctx's table to the subsystem */
            } else {
                /* restore the default for ctx's table */
            }
            co_return;
        });
```

`register_config_callback` registers on the calling shard only, and the manager invokes the callback only on that shard (see [Live Config Application](#live-config-application)). The returned future resolves to a registration handle, which must be kept alive for as long as the callback should remain registered; destroying it unregisters the callback. Register on a single shard to react once, or on every shard (`invoke_on_all`) for per-shard behavior. The callback mechanics are defined in [Live Config Application](#live-config-application).

#### Reading A Native Option

A native option is read either through a registered callback or by resolving it on demand. Both paths use the same `lookup_context` and resolution rules described in [Resolution Precedence](#resolution-precedence).


### Extending the Feature: Adding a Native Option

Use this path when the key is persisted in cluster config and consumed directly by a subsystem (it does not exist in `db/config.cc`).

1. Add the key to `db/cluster_config_registry.cc`.
2. Define which scopes it supports.
3. Set its `min_version` to the registry epoch its batch belongs to, and gate that epoch behind the matching `CLUSTER_CONFIG_REGISTRY_V*` feature (see [Registry Versioning](#registry-versioning)).
4. Decide whether its consumer reads the value on demand or needs a callback.
5. If it needs a callback, register one with the manager, keep the registration handle resolved by the returned future alive, and make the callback idempotent — `register_config_callback(name, on_change)` (see [Live Config Application](#live-config-application)).
6. Define clear parsing, defaulting, and restore-on-unset behavior for that consumer.
7. Add coverage for schema persistence, resolution, and any behavioral side effects.


### Tests

The shared test areas and main test files are listed in [Tests](#tests) above. Category-specific areas for native options:

- callback application and source-clearing behavior for native options with callbacks
- options without callbacks are persisted and resolved but cause no behavioral side effect
- registry validation and scope rejection for newly added native keys

The proposed v1 implementation should cover `auto_repair_enabled` in these areas: the table-oriented CQL surface and persistence in `test/boost/cql_query_test.cc`, and the schema-version/digest and prepared-statement behavior in `test/boost/schema_change_test.cc`.