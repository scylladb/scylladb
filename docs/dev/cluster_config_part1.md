# Cluster Config — Part 1: Native Cluster Config Options

This document is the implementation-oriented design and developer guide for **native cluster-config options**: keys that are designed for cluster config from the start and carry no legacy surface to preserve.

Cluster config supports three categories of options. Each has its own design document:

- Cluster Config — Part 1: Native Cluster Config Options (this document)
- Cluster Config — Part 2: Options Migrated From `db::config.cc`
- Cluster Config — Part 3: Options Migrated From Legacy Schema Properties

The shared model — scopes, storage, resolution, schema-backed ownership, the manager, callbacks, the CQL management surface, and tests — is defined in Part 0: Shared Foundation. Read Part 0 first. This document covers only what is specific to native cluster-config options.

## The Native-Option Model

A native cluster-config option is designed for cluster config from the start, rather than migrated from an existing schema property or an existing `db::config.cc` entry.

The model for native cluster-config options is:

- the registry defines the canonical option name, type, supported scopes, and minimum registry version
- the CQL surface uses the registry-defined name directly rather than preserving a legacy property spelling
- stored overrides use the same `configs`-based override model
- if you do not set the value at a scope, that scope keeps no override for it
- the effective value comes from the same normal scope resolution rules used for other cluster-config keys
- node behavior and `DESCRIBE` visibility are per-option policy decisions; a write's schema-version, digest, and prepared-statement consequences are uniform (see Part 0, Schema-Backed Ownership)
- if the owning subsystem registers a callback, the manager invokes it with the resolved value whenever it changes; otherwise the option remains persisted metadata read on demand

In short, native options use the cluster-config model directly: they do not inherit legacy schema-property or `db::config.cc` rules, but they share the same stored-override and scope-resolution model. Use this category whenever a key is designed for cluster config from the start with no legacy surface to preserve.

### Default Values

The registry owns a native option's name, type, supported scopes, and minimum registry version, but not its default. If no scope stores an override, `resolve_config` returns absence and the consuming subsystem applies its own built-in default. If a user explicitly stores the same value as that default, the override is still persisted in `configs`.

## External Surfaces

The shared CQL management surface and common validation rules are defined in Part 0.

A native option declares which scopes it supports in the registry, and the CQL surface uses the registry-defined name directly. `auto_repair_enabled` is registered for the schema scopes (`CLUSTER`, `KEYSPACE`, `TABLE`), so it is set through the table-oriented forms, with `CLUSTER` as the shared fallback:

```sql
ALTER CLUSTER WITH auto_repair_enabled = true;
ALTER KEYSPACE ks WITH auto_repair_enabled = false;
ALTER TABLE ks.tbl WITH auto_repair_enabled = true;
```

### Introspection Limits In The Proposed First Version

Read-back coverage depends on the option's scopes:

- a schema-scoped native option such as `auto_repair_enabled` is table-oriented: its effective value is rendered in `DESCRIBE KEYSPACE` and `DESCRIBE TABLE`, including inherited fallback, so it has a defined read-back path from day one.
- the node-oriented introspection surfaces — `DESCRIBE CLUSTER`, `DESCRIBE DATACENTER`, `DESCRIBE RACK`, and `DESCRIBE NODE` — are **not proposed for the first version** (see Part 0). A native option registered for the node-oriented scopes therefore has no `DESCRIBE` read-back surface in this iteration.

The effective value is emitted as an executable property and annotated with the full stored-override chain described in Part 0. The comment appears on its own line after the full `CREATE TABLE ... ;`. For `auto_repair_enabled`, whose chain is `table -> keyspace -> cluster`, `DESCRIBE TABLE ks.tbl` renders, for example:

```
... AND auto_repair_enabled = true;
-- effective from table, stored=[table=true, keyspace=false, cluster=NULL]

... AND auto_repair_enabled = false;
-- effective from keyspace, stored=[table=NULL, keyspace=false, cluster=true]

... AND auto_repair_enabled = true;
-- effective from cluster, stored=[table=NULL, keyspace=NULL, cluster=true]
```

`DESCRIBE KEYSPACE ks` describes the shorter `keyspace -> cluster` chain and omits the `table` entry.

Until the node-oriented `DESCRIBE` surfaces exist, the stored overrides for node-oriented scopes are still readable through the manager's per-scope getters (`get_cluster_config`, `get_dc_config`, `get_rack_config`, `get_node_config`) and by querying the `system_schema.scylla_clusters`, `system_schema.scylla_datacenters`, `system_schema.scylla_racks`, and `system_schema.scylla_nodes` tables directly. This is an explicit proposed first-version limitation, accepted because the only native option proposed for the first version (`auto_repair_enabled`) is schema-scoped and therefore already introspectable through `DESCRIBE`.

## Implementation Details

The shared registry, storage model, manager, resolution precedence, and callback model are defined in Part 0. The points below are specific to native options.

### Callbacks For Native Options

A native option that needs to react to changes registers a callback with the manager — the same interface every option uses (see Part 0):

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

`register_config_callback` registers on the calling shard only, and the manager invokes the callback only on that shard (see Part 0). The returned future resolves to a registration handle, which must be kept alive for as long as the callback should remain registered; destroying it unregisters the callback. Register on a single shard to react once, or on every shard (`invoke_on_all`) for per-shard behavior. The callback mechanics are defined in Part 0.

### Reading A Native Option

A native option is read either through a registered callback or by resolving it on demand. Both paths use the same `lookup_context` and resolution rules described in Part 0.


## Extending the Feature: Adding a Native Option

Use this path when the key is persisted in cluster config and consumed directly by a subsystem (it does not exist in `db/config.cc`).

1. Add the key to `db/cluster_config_registry.cc`.
2. Define which scopes it supports.
3. Set its `min_version` to the registry epoch its batch belongs to, and gate that epoch behind the matching `ALTER_CLUSTER_CONFIG_REGISTRY_V*` feature (see Part 0, Registry Versioning).
4. Decide whether its consumer reads the value on demand or needs a callback.
5. If it needs a callback, register one with the manager, keep the registration handle resolved by the returned future alive, and make the callback idempotent — `register_config_callback(name, on_change)` (see Part 0, Live Config Application).
6. Define clear parsing, defaulting, and restore-on-unset behavior for that consumer.
7. Add coverage for schema persistence, resolution, and any behavioral side effects.


## Tests

The shared test areas and main test files are listed in Part 0. Category-specific areas for native options:

- callback application and source-clearing behavior for native options with callbacks
- options without callbacks are persisted and resolved but cause no behavioral side effect
- registry validation and scope rejection for newly added native keys

The proposed v1 implementation should cover `auto_repair_enabled` in these areas: the table-oriented CQL surface and persistence in `test/boost/cql_query_test.cc`, and the schema-version/digest and prepared-statement behavior in `test/boost/schema_change_test.cc`.