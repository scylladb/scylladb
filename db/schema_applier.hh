/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "db/schema_tables.hh"
#include "mutation/mutation.hh"
#include "seastar/core/future.hh"
#include "service/storage_proxy.hh"
#include "query-result-set.hh"

#include <seastar/core/distributed.hh>

namespace db {

namespace schema_tables {

// Must be called on shard 0.
future<semaphore_units<>> hold_merge_lock() noexcept;

future<> merge_schema(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat, std::vector<mutation> mutations, bool reload = false);

// Recalculates the local schema version.
//
// It is safe to call concurrently with recalculate_schema_version() and merge_schema() in which case it
// is guaranteed that the schema version we end up with after all calls will reflect the most recent state
// of feature_service and schema tables.
future<> recalculate_schema_version(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat);

enum class table_kind { table, view };

struct table_selector {
    bool all_in_keyspace = false; // If true, selects all existing tables in a keyspace plus what's in "tables";
    std::unordered_map<table_kind, std::unordered_set<sstring>> tables;

    table_selector& operator+=(table_selector&& o);
    void add(table_kind t, sstring name);
    void add(sstring name);
};

struct schema_complete_view {
    schema_tables::schema_result keyspaces;
    std::map<table_id, schema_mutations> tables;
    schema_tables::schema_result types;
    std::map<table_id, schema_mutations> views;
    schema_tables::schema_result functions;
    schema_tables::schema_result aggregates;
    schema_tables::schema_result scylla_aggregates;
};

class schema_applier {
    using keyspace_name = sstring;

    sharded<service::storage_proxy>& _proxy;
    sharded<db::system_keyspace>& _sys_ks;
    bool _reload;

    std::set<sstring> _keyspaces;
    std::unordered_map<keyspace_name, table_selector> _affected_tables;
    bool _has_tablet_mutations = false;

    schema_complete_view _before;
    schema_complete_view _after;

    future<schema_complete_view> get_schema_complete_view();
public:
    schema_applier(
            sharded<service::storage_proxy>& proxy,
            sharded<db::system_keyspace>& sys_ks,
            bool reload = false)
            : _proxy(proxy), _sys_ks(sys_ks), _reload(reload) {};

    // Gets called before mutations are applied,
    // preferably no work should be done here but subsystem
    // may do some snapshot of 'before' data.
    future<> prepare(const std::vector<mutation>& muts);
    // Allows to modify mutations before they are applied,
    // preferably no work is done here but sometimes we need to adjust
    // for legacy things.
    future<> adjust(std::vector<mutation>& muts);
    // Update is called after mutations are applied, it should create
    // all updates but not yet commit them to a subsystem (i.e. copy on write style).
    future<> update(const std::vector<mutation>& muts);
    // Makes updates visible, it should be something very simple like a pointer switch and not yield so that it can be atomically composed with other subsystems.
    void commit();
    // Notify is called after commit and allows to trigger code which can't provide
    // atomicity either for legacy reasons or causes side effects to an external system
    // (e.g. informing client's driver).
    future<> notify();
};

}

}
