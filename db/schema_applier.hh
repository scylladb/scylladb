/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "schema/frozen_schema.hh"
#include "mutation/mutation.hh"
#include <seastar/core/future.hh>
#include "service/storage_proxy.hh"
#include "query/query-result-set.hh"
#include "db/schema_tables.hh"
#include "data_dictionary/user_types_metadata.hh"
#include "schema/schema_registry.hh"
#include "service/storage_service.hh"
#include "replica/database.hh"
#include "replica/global_table_ptr.hh"
#include "replica/tables_metadata_lock.hh"

#include <seastar/core/sharded.hh>
#include <unordered_map>

namespace db {

namespace schema_tables {

future<> merge_schema(sharded<db::system_keyspace>& sys_ks, sharded<service::storage_proxy>& proxy, sharded<service::storage_service>& ss, gms::feature_service& feat, utils::chunked_vector<mutation> mutations, bool reload = false);

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

struct affected_keyspaces_names {
    std::set<sstring> created;
    std::set<sstring> altered;
    std::set<sstring> dropped;
};

// groups keyspaces based on what is happening to them during schema change
struct affected_keyspaces {
    std::vector<replica::database::created_keyspace_per_shard> created;
    std::vector<replica::database::keyspace_change_per_shard> altered;
    // names need to be copied here as they are used multiple times and
    // keyspace struct from which we obtain the name is moved when
    // we commit it
    affected_keyspaces_names names;
};

struct affected_user_types_per_shard {
    std::vector<user_type> created;
    std::vector<user_type> altered;
    std::vector<user_type> dropped;
};

// groups UDTs based on what is happening to them during schema change
using affected_user_types = sharded<affected_user_types_per_shard>;

// In_progress_types_storage_per_shard contains current
// types with in-progress modifications applied.
// Important note: this storage can't be used directly in all cases,
// e.g. it's legal to drop type together with dropping other entity
// in such case we use existing storage instead so that whatever
// is being dropped can reference this type (we remove it from in_progress storage)
// in such cases get proper storage via committed_storage().
class in_progress_types_storage_per_shard : public data_dictionary::user_types_storage {
    std::shared_ptr<data_dictionary::user_types_storage> _stored_user_types;
    std::map<sstring, data_dictionary::user_types_metadata> _in_progress_types;
public:
    in_progress_types_storage_per_shard(replica::database& db, const affected_keyspaces& affected_keyspaces, const affected_user_types& affected_types);
    virtual const data_dictionary::user_types_metadata& get(const sstring& ks) const override;
    std::shared_ptr<data_dictionary::user_types_storage> committed_storage();
};

class in_progress_types_storage {
    // wrapped in foreign_ptr so they can be destroyed on the right shard
    std::vector<foreign_ptr<shared_ptr<in_progress_types_storage_per_shard>>> shards;
public:
    in_progress_types_storage() : shards(smp::count) {}
    future<> init(sharded<replica::database>& sharded_db, const affected_keyspaces& affected_keyspaces, const affected_user_types& affected_types);
    in_progress_types_storage_per_shard& local();
};

struct frozen_schema_diff {
    struct altered_schema {
        frozen_schema_with_base_info old_schema;
        frozen_schema_with_base_info new_schema;
    };
    std::vector<frozen_schema_with_base_info> created;
    std::vector<altered_schema> altered;
    std::vector<frozen_schema_with_base_info> dropped;
};

// schema_diff represents what is happening with tables or views during schema merge
struct schema_diff_per_shard {
    struct altered_schema {
        schema_ptr old_schema;
        schema_ptr new_schema;
    };

    std::vector<schema_ptr> created;
    std::vector<altered_schema> altered;
    std::vector<schema_ptr> dropped;

    future<frozen_schema_diff> freeze() const;

    static future<schema_diff_per_shard> copy_from(replica::database&, in_progress_types_storage&, const frozen_schema_diff& oth);
};


class new_token_metadata {
    std::vector<locator::mutable_token_metadata_ptr> shards{smp::count};
public:
    locator::mutable_token_metadata_ptr& local();
    future<> destroy();
};

struct affected_tables_and_views_per_shard {
    schema_diff_per_shard tables;
    schema_diff_per_shard views;
    std::vector<bool> columns_changed;
};

struct affected_tables_and_views {
    sharded<affected_tables_and_views_per_shard> tables_and_views;

    std::unique_ptr<replica::tables_metadata_lock_on_all_shards> locks;
    std::unordered_map<table_id, replica::global_table_ptr> table_shards;

    new_token_metadata new_token_metadata; // represents token metadata after updating tablets metadata, nullptr if there was no change
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
    sharded<service::storage_service>& _ss;
    sharded<db::system_keyspace>& _sys_ks;
    const bool _reload;

    std::set<sstring> _keyspaces;
    std::unordered_map<keyspace_name, table_selector> _affected_tables;
    locator::tablet_metadata_change_hint _tablet_hint;

    schema_persisted_state _before;
    schema_persisted_state _after;

    in_progress_types_storage _types_storage;

    affected_keyspaces _affected_keyspaces;
    affected_user_types _affected_user_types;
    affected_tables_and_views _affected_tables_and_views;

    functions_change_batch_all_shards _functions_batch; // includes aggregates

    future<schema_persisted_state> get_schema_persisted_state();
public:
    schema_applier(
            sharded<service::storage_proxy>& proxy,
            sharded<service::storage_service>& ss,
            sharded<db::system_keyspace>& sys_ks,
            bool reload = false)
            : _proxy(proxy), _ss(ss), _sys_ks(sys_ks), _reload(reload) {};

    // Gets called before mutations are applied,
    // preferably no work should be done here but subsystem
    // may do some snapshot of 'before' data.
    future<> prepare(utils::chunked_vector<mutation>& muts);
    // Update is called after mutations are applied, it should create
    // all updates but not yet commit them to a subsystem (i.e. copy on write style).
    // All changes should be visible only to schema_applier object but not to other subsystems.
    future<> update();
    // Makes updates visible. Before calling this function in memory state as observed by other
    // components should not yet change. The function atomically switches current state with
    // new state (the one built in update function).
    future<> commit();
    // Post_commit is called after commit and allows to trigger code which can't provide
    // atomicity either for legacy reasons or causes side effects to an external system
    // (e.g. informing client's driver).
    future<> post_commit();
    // Some destruction may need to be done on particular shard hence we need to run it in coroutine.
    future<> destroy();

private:
    void commit_tables_and_views();
    future<> finalize_tables_and_views();
    void commit_on_shard(replica::database& db);
};

}

}
