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
#include "data_dictionary/user_types_metadata.hh"
#include "schema/schema_registry.hh"

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

// groups keyspaces based on what is happening to them during schema change
struct affected_keyspaces {
    std::set<sstring> created;
    std::set<sstring> altered;
    std::set<sstring> dropped;
};

struct affected_user_types_per_shard {
    std::vector<user_type> created;
    std::vector<user_type> altered;
    std::vector<user_type> dropped;
};

// groups UDTs based on what is happening to them during schema change
using affected_user_types = sharded<affected_user_types_per_shard>;

// schema_diff represents what is happening with tables or views during schema merge
struct schema_diff {
    struct dropped_schema {
        global_schema_ptr schema;
    };

    struct altered_schema {
        global_schema_ptr old_schema;
        global_schema_ptr new_schema;
    };

    std::vector<global_schema_ptr> created;
    std::vector<altered_schema> altered;
    std::vector<dropped_schema> dropped;

    size_t size() const {
        return created.size() + altered.size() + dropped.size();
    }
};

struct affected_tables_and_views {
    schema_diff tables;
    schema_diff views;
    std::vector<bool> columns_changed;
};

// We wrap it with pointer because change_batch needs to be constructed and destructed
// on the same shard as it's used for.
using functions_change_batch_all_shards = sharded<cql3::functions::change_batch>;

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

    affected_keyspaces _affected_keyspaces;
    affected_user_types _affected_user_types;
    affected_tables_and_views _affected_tables_and_views;

    functions_change_batch_all_shards _functions_batch; // includes aggregates

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
    // Post_commit is called after commit and allows to trigger code which can't provide
    // atomicity either for legacy reasons or causes side effects to an external system
    // (e.g. informing client's driver).
    future<> post_commit();
    // Some destruction may need to be done on particular shard hence we need to run it in coroutine.
    future<> destroy();
};

}

}
