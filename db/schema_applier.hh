/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "mutation/mutation.hh"
#include <seastar/core/future.hh>
#include "service/storage_proxy.hh"
#include "query-result-set.hh"
#include "db/schema_tables.hh"

#include <seastar/core/distributed.hh>

namespace db {

namespace schema_tables {

future<> merge_schema(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat, std::vector<mutation> mutations, bool reload = false);

enum class table_kind { table, view };

struct table_selector {
    bool all_in_keyspace = false; // If true, selects all existing tables in a keyspace plus what's in "tables";
    std::unordered_map<table_kind, std::unordered_set<sstring>> tables;

    table_selector& operator+=(table_selector&& o);
    void add(table_kind t, sstring name);
    void add(sstring name);
};

struct schema_persisted_state {
    schema_tables::schema_result keyspaces;
    schema_tables::schema_result scylla_keyspaces;
    std::map<table_id, schema_mutations> tables;
    schema_tables::schema_result types;
    std::map<table_id, schema_mutations> views;
    schema_tables::schema_result functions;
    schema_tables::schema_result aggregates;
    schema_tables::schema_result scylla_aggregates;
};

// Schema_applier encapsulates intermediate state needed to construct schema objects from
// set of rows read from system tables (see struct schema_state). It does atomic (per shard)
// application of a new schema.
class schema_applier {
    using keyspace_name = sstring;

    sharded<service::storage_proxy>& _proxy;
    sharded<db::system_keyspace>& _sys_ks;
    const bool _reload;

    std::set<sstring> _keyspaces;
    std::unordered_map<keyspace_name, table_selector> _affected_tables;
    locator::tablet_metadata_change_hint _tablet_hint;

    schema_persisted_state _before;
    schema_persisted_state _after;

    future<schema_persisted_state> get_schema_persisted_state();
public:
    schema_applier(
            sharded<service::storage_proxy>& proxy,
            sharded<db::system_keyspace>& sys_ks,
            bool reload = false)
            : _proxy(proxy), _sys_ks(sys_ks), _reload(reload) {};

    // Gets called before mutations are applied,
    // preferably no work should be done here but subsystem
    // may do some snapshot of 'before' data.
    future<> prepare(std::vector<mutation>& muts);
    // Update is called after mutations are applied, it should create
    // all updates but not yet commit them to a subsystem (i.e. copy on write style).
    // All changes should be visible only to schema_applier object but not to other subsystems.
    future<> update();
    // Makes updates visible. Before calling this function in memory state as observed by other
    // components should not yet change. The function atomically switches current state with
    // new state (the one built in update function).
    void commit();
    // Notify is called after commit and allows to trigger code which can't provide
    // atomicity either for legacy reasons or causes side effects to an external system
    // (e.g. informing client's driver).
    future<> notify();
};

}

}
