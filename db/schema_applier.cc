/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "schema_applier.hh"

#include <seastar/util/noncopyable_function.hh>
#include <seastar/rpc/rpc_types.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/on_internal_error.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/transform.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/join.hpp>

#include <fmt/ranges.h>

#include "view_info.hh"
#include "replica/database.hh"
#include "lang/manager.hh"
#include "db/system_keyspace.hh"
#include "cql3/expr/expression.hh"
#include "types/types.hh"
#include "db/schema_tables.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "gms/feature_service.hh"
#include "dht/i_partitioner.hh"
#include "system_keyspace.hh"
#include "query-result-set.hh"
#include "query-result-writer.hh"
#include "map_difference.hh"
#include <seastar/coroutine/all.hh>
#include "utils/log.hh"
#include "frozen_schema.hh"
#include "schema/schema_registry.hh"
#include "system_keyspace.hh"
#include "system_distributed_keyspace.hh"
#include "cql3/query_processor.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "mutation/async_utils.hh"
#include "db/schema_tables.hh"

namespace db {

namespace schema_tables {

enum class table_kind { table, view };

static constexpr std::initializer_list<table_kind> all_table_kinds = {
    table_kind::table,
    table_kind::view
};

static schema_ptr get_table_holder(table_kind k) {
    switch (k) {
        case table_kind::table: return tables();
        case table_kind::view: return views();
    }
    abort();
}

}

}

template <> struct fmt::formatter<db::schema_tables::table_kind> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(db::schema_tables::table_kind k, fmt::format_context& ctx) const {
        switch (k) {
        using enum db::schema_tables::table_kind;
        case table:
            return fmt::format_to(ctx.out(), "table");
        case view:
            return fmt::format_to(ctx.out(), "view");
        }
        abort();
    }
};

namespace db {

namespace schema_tables {

struct table_selector {
    bool all_in_keyspace = false; // If true, selects all existing tables in a keyspace plus what's in "tables";
    std::unordered_map<table_kind, std::unordered_set<sstring>> tables;

    table_selector& operator+=(table_selector&& o) {
        all_in_keyspace |= o.all_in_keyspace;
        for (auto t : all_table_kinds) {
            tables[t].merge(std::move(o.tables[t]));
        }
        return *this;
    }

    void add(table_kind t, sstring name) {
        tables[t].emplace(std::move(name));
    }

    void add(sstring name) {
        for (auto t : all_table_kinds) {
            add(t, name);
        }
    }
};

static std::optional<table_id> table_id_from_mutations(const schema_mutations& sm) {
    auto table_rs = query::result_set(sm.columnfamilies_mutation());
    if (table_rs.empty()) {
        return std::nullopt;
    }
    query::result_set_row table_row = table_rs.row(0);
    return table_id(table_row.get_nonnull<utils::UUID>("id"));
}

static
future<std::map<table_id, schema_mutations>>
read_tables_for_keyspaces(distributed<service::storage_proxy>& proxy, const std::set<sstring>& keyspace_names, table_kind kind,
                          const std::unordered_map<sstring, table_selector>& tables_per_keyspace)
{
    std::map<table_id, schema_mutations> result;
    for (auto&& [keyspace_name, sel] : tables_per_keyspace) {
        if (!sel.tables.contains(kind)) {
            continue;
        }
        for (auto&& table_name : sel.tables.find(kind)->second) {
            auto qn = qualified_name(keyspace_name, table_name);
            auto muts = co_await read_table_mutations(proxy, qn, get_table_holder(kind));
            auto id = table_id_from_mutations(muts);
            if (id) {
                result.emplace(std::move(*id), std::move(muts));
            }
        }
    }
    co_return result;
}

// Extracts the names of tables affected by a schema mutation.
// The mutation must target one of the tables in schema_tables_holding_schema_mutations().
static
table_selector get_affected_tables(const sstring& keyspace_name, const mutation& m) {
    const schema& s = *m.schema();
    auto get_table_name = [&] (const clustering_key& ck) {
        // The first component of the clustering key in each table listed in
        // schema_tables_holding_schema_mutations contains the table name.
        return value_cast<sstring>(utf8_type->deserialize(ck.get_component(s, 0)));
    };
    table_selector result;
    if (m.partition().partition_tombstone()) {
        slogger.trace("Mutation of {}.{} for keyspace {} contains a partition tombstone",
                      m.schema()->ks_name(), m.schema()->cf_name(), keyspace_name);
        result.all_in_keyspace = true;
    }
    for (auto&& e : m.partition().row_tombstones()) {
        const range_tombstone& rt = e.tombstone();
        if (rt.start.size(s) == 0 || rt.end.size(s) == 0) {
            slogger.trace("Mutation of {}.{} for keyspace {} contains a multi-table range tombstone",
                          m.schema()->ks_name(), m.schema()->cf_name(), keyspace_name);
            result.all_in_keyspace = true;
            break;
        }
        auto table_name = get_table_name(rt.start);
        if (table_name != get_table_name(rt.end)) {
            slogger.trace("Mutation of {}.{} for keyspace {} contains a multi-table range tombstone",
                          m.schema()->ks_name(), m.schema()->cf_name(), keyspace_name);
            result.all_in_keyspace = true;
            break;
        }
        result.add(table_name);
    }
    for (auto&& row : m.partition().clustered_rows()) {
        result.add(get_table_name(row.key()));
    }
    slogger.trace("Mutation of {}.{} for keyspace {} affects tables: {}, all_in_keyspace: {}",
                  m.schema()->ks_name(), m.schema()->cf_name(), keyspace_name, result.tables, result.all_in_keyspace);
    return result;
}

future<schema_result>
static read_schema_for_keyspaces(distributed<service::storage_proxy>& proxy, const sstring& schema_table_name, const std::set<sstring>& keyspace_names)
{
    auto map = [&proxy, schema_table_name] (const sstring& keyspace_name) { return read_schema_partition_for_keyspace(proxy, schema_table_name, keyspace_name); };
    auto insert = [] (schema_result&& result, auto&& schema_entity) {
        if (!schema_entity.second->empty()) {
            result.insert(std::move(schema_entity));
        }
        return std::move(result);
    };
    co_return co_await map_reduce(keyspace_names.begin(), keyspace_names.end(), map, schema_result{}, insert);
}

// Returns names of live table definitions of given keyspace
future<std::vector<sstring>>
static read_table_names_of_keyspace(distributed<service::storage_proxy>& proxy, const sstring& keyspace_name, schema_ptr schema_table) {
    auto pkey = dht::decorate_key(*schema_table, partition_key::from_singular(*schema_table, keyspace_name));
    auto&& rs = co_await db::system_keyspace::query(proxy.local().get_db(), schema_table->ks_name(), schema_table->cf_name(), pkey);
    co_return rs->rows() | std::views::transform([schema_table] (const query::result_set_row& row) {
        const sstring name = schema_table->clustering_key_columns().begin()->name_as_text();
        return row.get_nonnull<sstring>(name);
    }) | std::ranges::to<std::vector>();
}

// Applies deletion of the "version" column to system_schema.scylla_tables mutation rows
// which weren't committed by group 0.
static void maybe_delete_schema_version(mutation& m) {
    if (m.column_family_id() != scylla_tables()->id()) {
        return;
    }
    const column_definition& origin_col = *m.schema()->get_column_definition(to_bytes("committed_by_group0"));
    const column_definition& version_col = *m.schema()->get_column_definition(to_bytes("version"));
    for (auto&& row : m.partition().clustered_rows()) {
        auto&& cells = row.row().cells();
        if (auto&& origin_cell = cells.find_cell(origin_col.id); origin_cell) {
            auto&& ac = origin_cell->as_atomic_cell(origin_col);
            if (ac.is_live()) {
                auto dv = origin_col.type->deserialize(managed_bytes_view(ac.value()));
                auto committed_by_group0 = value_cast<bool>(dv);
                if (committed_by_group0) {
                    // Don't delete "version" for this entry.
                    continue;
                }
            }
        }
        auto&& cell = cells.find_cell(version_col.id);
        api::timestamp_type t = api::new_timestamp();
        if (cell) {
            t = std::max(t, cell->as_atomic_cell(version_col).timestamp());
        }
        cells.apply(version_col, atomic_cell::make_dead(t, gc_clock::now()));
    }
}

static future<std::set<sstring>> merge_keyspaces(distributed<service::storage_proxy>& proxy,
                                                 const schema_result& before, const schema_result& after,
                                                 const schema_result& sk_before, const schema_result& sk_after)
{
    /*
     * - we don't care about entriesOnlyOnLeft() or entriesInCommon(), because only the changes are of interest to us
     * - of all entriesOnlyOnRight(), we only care about ones that have live columns; it's possible to have a ColumnFamily
     *   there that only has the top-level deletion, if:
     *      a) a pushed DROP KEYSPACE change for a keyspace hadn't ever made it to this node in the first place
     *      b) a pulled dropped keyspace that got dropped before it could find a way to this node
     * - of entriesDiffering(), we don't care about the scenario where both pre and post-values have zero live columns:
     *   that means that a keyspace had been recreated and dropped, and the recreated keyspace had never found a way
     *   to this node
     */
    auto diff = difference(before, after, indirect_equal_to<lw_shared_ptr<query::result_set>>());
    auto sk_diff = difference(sk_before, sk_after, indirect_equal_to<lw_shared_ptr<query::result_set>>());

    auto& created = diff.entries_only_on_right;
    auto& altered = diff.entries_differing;
    auto& dropped = diff.entries_only_on_left;

    // For the ALTER case, we have to also consider changes made to SCYLLA_KEYSPACES, not only to KEYSPACES:
    // 1. changes made to non-null columns...
    altered.insert(sk_diff.entries_differing.begin(), sk_diff.entries_differing.end());
    // 2. ... and new or deleted entries - these change only when ALTERing, not CREATE'ing or DROP'ing
    for (auto&& ks : boost::range::join(sk_diff.entries_only_on_right, sk_diff.entries_only_on_left)) {
        if (!created.contains(ks) && !dropped.contains(ks)) {
            altered.emplace(ks);
        }
    }

    auto& sharded_db = proxy.local().get_db();
    for (auto& name : created) {
        slogger.info("Creating keyspace {}", name);
        auto sk_after_v = sk_after.contains(name) ? sk_after.at(name) : nullptr;
        auto ksm = co_await create_keyspace_from_schema_partition(proxy,
                schema_result_value_type{name, after.at(name)}, sk_after_v);
        co_await replica::database::create_keyspace_on_all_shards(sharded_db, proxy, *ksm);
    }
    for (auto& name : altered) {
        slogger.info("Altering keyspace {}", name);
        auto sk_after_v = sk_after.contains(name) ? sk_after.at(name) : nullptr;
        auto tmp_ksm = co_await create_keyspace_from_schema_partition(proxy,
                schema_result_value_type{name, after.at(name)}, sk_after_v);
        co_await replica::database::update_keyspace_on_all_shards(sharded_db, *tmp_ksm);
    }
    for (auto& key : dropped) {
        slogger.info("Dropping keyspace {}", key);
    }
    co_return dropped;
}

static std::vector<const query::result_set_row*> collect_rows(const std::set<sstring>& keys, const schema_result& result) {
    std::vector<const query::result_set_row*> ret;
    for (const auto& key : keys) {
        for (const auto& row : result.find(key)->second->rows()) {
            ret.push_back(&row);
        }
    }
    return ret;
}

static std::vector<column_definition> get_primary_key_definition(const schema_ptr& schema) {
    std::vector<column_definition> primary_key;
    for (const auto& column : schema->partition_key_columns()) {
        primary_key.push_back(column);
    }
    for (const auto& column : schema->clustering_key_columns()) {
        primary_key.push_back(column);
    }

    return primary_key;
}

static std::vector<bytes> get_primary_key(const std::vector<column_definition>& primary_key, const query::result_set_row* row) {
    std::vector<bytes> key;
    for (const auto& column : primary_key) {
        const data_value *val = row->get_data_value(column.name_as_text());
        key.push_back(val->serialize_nonnull());
    }
    return key;
}

// Build a map from primary keys to rows.
static std::map<std::vector<bytes>, const query::result_set_row*> build_row_map(const query::result_set& result) {
    const std::vector<query::result_set_row>& rows = result.rows();
    auto primary_key = get_primary_key_definition(result.schema());
    std::map<std::vector<bytes>, const query::result_set_row*> ret;
    for (const auto& row: rows) {
        auto key = get_primary_key(primary_key, &row);
        ret.insert(std::pair(std::move(key), &row));
    }
    return ret;
}

struct row_diff {
    std::vector<const query::result_set_row*> altered;
    std::vector<const query::result_set_row*> created;
    std::vector<const query::result_set_row*> dropped;
};

// Compute which rows have been created, dropped or altered.
// A row is identified by its primary key.
// In the output, all entries of a given keyspace are together.
static row_diff diff_rows(const schema_result& before, const schema_result& after) {
    auto diff = difference(before, after, indirect_equal_to<lw_shared_ptr<query::result_set>>());

    // For new or empty keyspaces, just record each row.
    auto dropped = collect_rows(diff.entries_only_on_left, before); // Keyspaces now without rows
    auto created = collect_rows(diff.entries_only_on_right, after); // New keyspaces with rows
    std::vector<const query::result_set_row*> altered;

    for (const auto& key : diff.entries_differing) {
        // For each keyspace that changed, compute the difference of the corresponding result_set to find which rows
        // have changed.
        auto before_rows = build_row_map(*before.find(key)->second);
        auto after_rows = build_row_map(*after.find(key)->second);
        auto diff_row = difference(before_rows, after_rows, indirect_equal_to<const query::result_set_row*>());
        for (const auto& key : diff_row.entries_only_on_left) {
            dropped.push_back(before_rows.find(key)->second);
        }
        for (const auto& key : diff_row.entries_only_on_right) {
            created.push_back(after_rows.find(key)->second);
        }
        for (const auto& key : diff_row.entries_differing) {
            altered.push_back(after_rows.find(key)->second);
        }
    }
    return {std::move(altered), std::move(created), std::move(dropped)};
}

// User-defined aggregate stores its information in two tables: aggregates and scylla_aggregates
// The difference has to be joined to properly create an UDA.
//
// FIXME: Since UDA cannot be altered now, set of differing rows should be empty and those rows are
// ignored in calculating the diff.
struct aggregate_diff {
    std::vector<std::pair<const query::result_set_row*, const query::result_set_row*>> created;
    std::vector<std::pair<const query::result_set_row*, const query::result_set_row*>> dropped;
};

static aggregate_diff diff_aggregates_rows(const schema_result& aggr_before, const schema_result& aggr_after,
        const schema_result& scylla_aggr_before, const schema_result& scylla_aggr_after) {
    using map = std::map<std::vector<bytes>, const query::result_set_row*>;
    auto aggr_diff = difference(aggr_before, aggr_after, indirect_equal_to<lw_shared_ptr<query::result_set>>());

    std::vector<std::pair<const query::result_set_row*, const query::result_set_row*>> created;
    std::vector<std::pair<const query::result_set_row*, const query::result_set_row*>> dropped;

    // Primary key for `aggregates` and `scylla_aggregates` tables
    auto primary_key = get_primary_key_definition(aggregates());

    // DROPPED
    for (const auto& key : aggr_diff.entries_only_on_left) {
        auto scylla_entry = scylla_aggr_before.find(key);
        auto scylla_aggr_rows = (scylla_entry != scylla_aggr_before.end()) ? build_row_map(*scylla_entry->second) : map();

        for (const auto& row : aggr_before.find(key)->second->rows()) {
            auto pk = get_primary_key(primary_key, &row);
            auto entry = scylla_aggr_rows.find(pk);
            dropped.push_back({&row, (entry != scylla_aggr_rows.end()) ? entry->second : nullptr});
        }
    }
    // CREATED
    for (const auto& key : aggr_diff.entries_only_on_right) {
        auto scylla_entry = scylla_aggr_after.find(key);
        auto scylla_aggr_rows = (scylla_entry != scylla_aggr_after.end()) ? build_row_map(*scylla_entry->second) : map();

        for (const auto& row : aggr_after.find(key)->second->rows()) {
            auto pk = get_primary_key(primary_key, &row);
            auto entry = scylla_aggr_rows.find(pk);
            created.push_back({&row, (entry != scylla_aggr_rows.end()) ? entry->second : nullptr});
        }
    }
    for (const auto& key : aggr_diff.entries_differing) {
        auto aggr_before_rows = build_row_map(*aggr_before.find(key)->second);
        auto aggr_after_rows = build_row_map(*aggr_after.find(key)->second);
        auto diff = difference(aggr_before_rows, aggr_after_rows, indirect_equal_to<const query::result_set_row*>());

        auto scylla_entry_before = scylla_aggr_before.find(key);
        auto scylla_aggr_rows_before = (scylla_entry_before != scylla_aggr_before.end()) ? build_row_map(*scylla_entry_before->second) : map();
        auto scylla_entry_after = scylla_aggr_after.find(key);
        auto scylla_aggr_rows_after = (scylla_entry_after != scylla_aggr_after.end()) ? build_row_map(*scylla_entry_after->second) : map();

        for (const auto& k : diff.entries_only_on_left) {
            auto entry = scylla_aggr_rows_before.find(k);
            dropped.push_back({
                aggr_before_rows.find(k)->second, (entry != scylla_aggr_rows_before.end()) ? entry->second : nullptr
            });
        }
        for (const auto& k : diff.entries_only_on_right) {
            auto entry = scylla_aggr_rows_after.find(k);
            created.push_back({
                aggr_after_rows.find(k)->second, (entry != scylla_aggr_rows_after.end()) ? entry->second : nullptr
            });
        }
    }

    return {std::move(created), std::move(dropped)};
}

struct [[nodiscard]] user_types_to_drop final {
    seastar::noncopyable_function<future<> ()> drop;
};

// see the comments for merge_keyspaces()
static future<user_types_to_drop> merge_types(distributed<service::storage_proxy>& proxy, schema_result before, schema_result after)
{
    auto diff = diff_rows(before, after);

    // Create and update user types before any tables/views are created that potentially
    // use those types. Similarly, defer dropping until after tables/views that may use
    // some of these user types are dropped.

    co_await proxy.local().get_db().invoke_on_all([&] (replica::database& db) -> future<> {
        auto created_types = co_await create_types(db, diff.created);
        for (auto&& user_type : created_types) {
            db.find_keyspace(user_type->_keyspace).add_user_type(user_type);
            co_await db.get_notifier().create_user_type(user_type);
        }
        auto altered_types = co_await create_types(db, diff.altered);
        for (auto&& user_type : altered_types) {
            db.find_keyspace(user_type->_keyspace).add_user_type(user_type);
            co_await db.get_notifier().update_user_type(user_type);
        }
    });

    co_return user_types_to_drop{[&proxy, before = std::move(before), rows = std::move(diff.dropped)] () mutable -> future<> {
        co_await proxy.local().get_db().invoke_on_all([&] (replica::database& db) -> future<> {
            auto dropped = co_await create_types(db, rows);
            for (auto& user_type : dropped) {
                db.find_keyspace(user_type->_keyspace).remove_user_type(user_type);
                co_await db.get_notifier().drop_user_type(user_type);
            }
        });
    }};
}

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

// Which side of the diff this schema is on?
// Helps ensuring that when creating schema for altered views, we match "before"
// version of view to "before" version of base table and "after" to "after"
// respectively.
enum class schema_diff_side {
    left, // old, before
    right, // new, after
};

static schema_diff diff_table_or_view(distributed<service::storage_proxy>& proxy,
    const std::map<table_id, schema_mutations>& before,
    const std::map<table_id, schema_mutations>& after,
    bool reload,
    noncopyable_function<schema_ptr (schema_mutations sm, schema_diff_side)> create_schema)
{
    schema_diff d;
    auto diff = difference(before, after);
    for (auto&& key : diff.entries_only_on_left) {
        auto&& s = proxy.local().get_db().local().find_schema(key);
        slogger.info("Dropping {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
        d.dropped.emplace_back(schema_diff::dropped_schema{s});
    }
    for (auto&& key : diff.entries_only_on_right) {
        auto s = create_schema(std::move(after.at(key)), schema_diff_side::right);
        slogger.info("Creating {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
        d.created.emplace_back(s);
    }
    for (auto&& key : diff.entries_differing) {
        auto s_before = create_schema(std::move(before.at(key)), schema_diff_side::left);
        auto s = create_schema(std::move(after.at(key)), schema_diff_side::right);
        slogger.info("Altering {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
        d.altered.emplace_back(schema_diff::altered_schema{s_before, s});
    }
    if (reload) {
        for (auto&& key: diff.entries_in_common) {
            auto s = create_schema(std::move(after.at(key)), schema_diff_side::right);
            slogger.info("Reloading {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
            d.altered.emplace_back(schema_diff::altered_schema {s, s});
        }
    }
    return d;
}

// Limit concurrency of user tables to prevent stalls.
// See https://github.com/scylladb/scylladb/issues/11574
// Note: we aim at providing enough concurrency to utilize
// the cpu while operations are blocked on disk I/O
// and or filesystem calls, e.g. fsync.
constexpr size_t max_concurrent = 8;

// see the comments for merge_keyspaces()
// Atomically publishes schema changes. In particular, this function ensures
// that when a base schema and a subset of its views are modified together (i.e.,
// upon an alter table or alter type statement), then they are published together
// as well, without any deferring in-between.
static future<> merge_tables_and_views(distributed<service::storage_proxy>& proxy,
    sharded<db::system_keyspace>& sys_ks,
    const std::map<table_id, schema_mutations>& tables_before,
    const std::map<table_id, schema_mutations>& tables_after,
    const std::map<table_id, schema_mutations>& views_before,
    const std::map<table_id, schema_mutations>& views_after,
    bool reload,
    locator::tablet_metadata_change_hint tablet_hint)
{
    auto tables_diff = diff_table_or_view(proxy, std::move(tables_before), std::move(tables_after), reload, [&] (schema_mutations sm, schema_diff_side) {
        return create_table_from_mutations(proxy, std::move(sm));
    });
    auto views_diff = diff_table_or_view(proxy, std::move(views_before), std::move(views_after), reload, [&] (schema_mutations sm, schema_diff_side side) {
        // The view schema mutation should be created with reference to the base table schema because we definitely know it by now.
        // If we don't do it we are leaving a window where write commands to this schema are illegal.
        // There are 3 possibilities:
        // 1. The table was altered - in this case we want the view to correspond to this new table schema.
        // 2. The table was just created - the table is guaranteed to be published with the view in that case.
        // 3. The view itself was altered - in that case we already know the base table so we can take it from
        //    the database object.
        view_ptr vp = create_view_from_mutations(proxy, std::move(sm));
        schema_ptr base_schema;
        for (auto&& altered : tables_diff.altered) {
            // Chose the appropriate version of the base table schema: old -> old, new -> new.
            schema_ptr s = side == schema_diff_side::left ? altered.old_schema : altered.new_schema;
            if (s->ks_name() == vp->ks_name() && s->cf_name() == vp->view_info()->base_name() ) {
                base_schema = s;
                break;
            }
        }
        if (!base_schema) {
            for (auto&& s : tables_diff.created) {
                if (s.get()->ks_name() == vp->ks_name() && s.get()->cf_name() == vp->view_info()->base_name() ) {
                    base_schema = s;
                    break;
                }
            }
        }

        if (!base_schema) {
            base_schema = proxy.local().local_db().find_schema(vp->ks_name(), vp->view_info()->base_name());
        }

        // Now when we have a referenced base - sanity check that we're not registering an old view
        // (this could happen when we skip multiple major versions in upgrade, which is unsupported.)
        check_no_legacy_secondary_index_mv_schema(proxy.local().get_db().local(), vp, base_schema);

        vp->view_info()->set_base_info(vp->view_info()->make_base_dependent_view_info(*base_schema));
        return vp;
    });

    // First drop views and *only then* the tables, if interleaved it can lead
    // to a mv not finding its schema when snapshotting since the main table
    // was already dropped (see https://github.com/scylladb/scylla/issues/5614)
    auto& db = proxy.local().get_db();
    co_await max_concurrent_for_each(views_diff.dropped, max_concurrent, [&db, &sys_ks] (schema_diff::dropped_schema& dt) {
        auto& s = *dt.schema.get();
        return replica::database::drop_table_on_all_shards(db, sys_ks, s.ks_name(), s.cf_name());
    });
    co_await max_concurrent_for_each(tables_diff.dropped, max_concurrent, [&db, &sys_ks] (schema_diff::dropped_schema& dt) -> future<> {
        auto& s = *dt.schema.get();
        return replica::database::drop_table_on_all_shards(db, sys_ks, s.ks_name(), s.cf_name());
    });

    if (tablet_hint) {
        slogger.info("Tablet metadata changed");
        // We must do it after tables are dropped so that table snapshot doesn't experience missing tablet map,
        // and so that compaction groups are not destroyed altogether.
        // We must also do it before tables are created so that new tables see the tablet map.
        co_await db.invoke_on_all([&] (replica::database& db) -> future<> {
            co_await db.get_notifier().update_tablet_metadata(std::move(tablet_hint));
        });
    }

    co_await db.invoke_on_all([&] (replica::database& db) -> future<> {
        // In order to avoid possible races we first create the tables and only then the views.
        // That way if a view seeks information about its base table it's guaranteed to find it.
        co_await max_concurrent_for_each(tables_diff.created, max_concurrent, [&] (global_schema_ptr& gs) -> future<> {
            co_await db.add_column_family_and_make_directory(gs, replica::database::is_new_cf::yes);
        });
        co_await max_concurrent_for_each(views_diff.created, max_concurrent, [&] (global_schema_ptr& gs) -> future<> {
            co_await db.add_column_family_and_make_directory(gs, replica::database::is_new_cf::yes);
        });
    });
    co_await db.invoke_on_all([&](replica::database& db) -> future<> {
        std::vector<bool> columns_changed;
        columns_changed.reserve(tables_diff.altered.size() + views_diff.altered.size());
        for (auto&& altered : boost::range::join(tables_diff.altered, views_diff.altered)) {
            columns_changed.push_back(db.update_column_family(altered.new_schema));
            co_await coroutine::maybe_yield();
        }
        auto it = columns_changed.begin();
        auto notify = [&] (auto& r, auto&& f) -> future<> {
            co_await max_concurrent_for_each(r, max_concurrent, std::move(f));
        };
        // View drops are notified first, because a table can only be dropped if its views are already deleted
        co_await notify(views_diff.dropped, [&] (auto&& dt) { return db.get_notifier().drop_view(view_ptr(dt.schema)); });
        co_await notify(tables_diff.dropped, [&] (auto&& dt) { return db.get_notifier().drop_column_family(dt.schema); });
        // Table creations are notified first, in case a view is created right after the table
        co_await notify(tables_diff.created, [&] (auto&& gs) { return db.get_notifier().create_column_family(gs); });
        co_await notify(views_diff.created, [&] (auto&& gs) { return db.get_notifier().create_view(view_ptr(gs)); });
        // Table altering is notified first, in case new base columns appear
        co_await notify(tables_diff.altered, [&] (auto&& altered) { return db.get_notifier().update_column_family(altered.new_schema, *it++); });
        co_await notify(views_diff.altered, [&] (auto&& altered) { return db.get_notifier().update_view(view_ptr(altered.new_schema), *it++); });
    });

    // Insert column_mapping into history table for altered and created tables.
    //
    // Entries for new tables are inserted without TTL, which means that the most
    // recent schema version should always be available.
    //
    // For altered tables we both insert a new column mapping without TTL and
    // overwrite the previous version entries with TTL to expire them eventually.
    //
    // Drop column mapping entries for dropped tables since these will not be TTLed automatically
    // and will stay there forever if we don't clean them up manually
    co_await max_concurrent_for_each(tables_diff.created, max_concurrent, [&proxy] (global_schema_ptr& gs) -> future<> {
        co_await store_column_mapping(proxy, gs.get(), false);
    });
    co_await max_concurrent_for_each(tables_diff.altered, max_concurrent, [&proxy] (schema_diff::altered_schema& altered) -> future<> {
        co_await when_all_succeed(
            store_column_mapping(proxy, altered.old_schema.get(), true),
            store_column_mapping(proxy, altered.new_schema.get(), false));
    });
    co_await max_concurrent_for_each(tables_diff.dropped, max_concurrent, [&sys_ks] (schema_diff::dropped_schema& dropped) -> future<> {
        schema_ptr s = dropped.schema.get();
        co_await drop_column_mapping(sys_ks.local(), s->id(), s->version());
    });
}

static void drop_cached_func(replica::database& db, const query::result_set_row& row) {
    auto language = row.get_nonnull<sstring>("language");
    if (language == "wasm") {
        cql3::functions::function_name name{
            row.get_nonnull<sstring>("keyspace_name"), row.get_nonnull<sstring>("function_name")};
        auto arg_types = read_arg_types(db, row, name.keyspace);
        db.lang().remove(name, arg_types);
    }
}

static future<> merge_functions(distributed<service::storage_proxy>& proxy, schema_result before, schema_result after) {
    auto diff = diff_rows(before, after);

    co_await proxy.local().get_db().invoke_on_all(coroutine::lambda([&] (replica::database& db) -> future<> {
        cql3::functions::change_batch batch;
        for (const auto& val : diff.created) {
            batch.add_function(co_await create_func(db, *val));
        }
        auto events = make_ready_future<>();
        for (const auto& val : diff.dropped) {
            cql3::functions::function_name name{
                val->get_nonnull<sstring>("keyspace_name"), val->get_nonnull<sstring>("function_name")};
            auto arg_types = read_arg_types(db, *val, name.keyspace);
            // as we don't yield between dropping cache and committing batch
            // change there is no window between cache removal and declaration removal
            drop_cached_func(db, *val);
            batch.remove_function(name, arg_types);
            events = events.then([&db, name, arg_types] () {
                return db.get_notifier().drop_function(std::move(name), std::move(arg_types));
            });
        }
        for (const auto& val : diff.altered) {
            drop_cached_func(db, *val);
            batch.replace_function(co_await create_func(db, *val));
        }
        batch.commit();
        co_await std::move(events);
    }));
}

static future<> merge_aggregates(distributed<service::storage_proxy>& proxy, const schema_result& before, const schema_result& after,
        const schema_result& scylla_before, const schema_result& scylla_after) {
    auto diff = diff_aggregates_rows(before, after, scylla_before, scylla_after);

    co_await proxy.local().get_db().invoke_on_all([&] (replica::database& db)-> future<> {
        cql3::functions::change_batch batch;
        for (const auto& val : diff.created) {
            batch.add_function(create_aggregate(db, *val.first, val.second, batch));
        }
        auto events = make_ready_future<>();
        for (const auto& val : diff.dropped) {
            cql3::functions::function_name name{
                val.first->get_nonnull<sstring>("keyspace_name"), val.first->get_nonnull<sstring>("aggregate_name")};
            auto arg_types = read_arg_types(db, *val.first, name.keyspace);
            batch.remove_function(name, arg_types);
            events = events.then([&db, name, arg_types] () {
                return db.get_notifier().drop_aggregate(std::move(name), std::move(arg_types));
            });
        }
        batch.commit();
        co_await std::move(events);
    });
}

static future<> do_merge_schema(distributed<service::storage_proxy>& proxy, sharded<db::system_keyspace>& sys_ks, std::vector<mutation> mutations, bool reload)
{
    slogger.trace("do_merge_schema: {}", mutations);
    schema_ptr s = keyspaces();
    // compare before/after schemas of the affected keyspaces only
    std::set<sstring> keyspaces;
    using keyspace_name = sstring;
    std::unordered_map<keyspace_name, table_selector> affected_tables;
    locator::tablet_metadata_change_hint tablet_hint;
    for (auto&& mutation : mutations) {
        sstring keyspace_name = value_cast<sstring>(utf8_type->deserialize(mutation.key().get_component(*s, 0)));

        if (schema_tables_holding_schema_mutations().contains(mutation.schema()->id())) {
            affected_tables[keyspace_name] += get_affected_tables(keyspace_name, mutation);
        }

        replica::update_tablet_metadata_change_hint(tablet_hint, mutation);

        keyspaces.emplace(std::move(keyspace_name));
        // We must force recalculation of schema version after the merge, since the resulting
        // schema may be a mix of the old and new schemas, with the exception of entries
        // that originate from group 0.
        maybe_delete_schema_version(mutation);
    }

    if (reload) {
        for (auto&& ks : proxy.local().get_db().local().get_non_system_keyspaces()) {
            keyspaces.emplace(ks);
            table_selector sel;
            sel.all_in_keyspace = true;
            affected_tables[ks] = sel;
        }
    }

    // Resolve sel.all_in_keyspace == true to the actual list of tables and views.
    for (auto&& [keyspace_name, sel] : affected_tables) {
        if (sel.all_in_keyspace) {
            // FIXME: Obtain from the database object
            slogger.trace("Reading table list for keyspace {}", keyspace_name);
            for (auto k : all_table_kinds) {
                for (auto&& n : co_await read_table_names_of_keyspace(proxy, keyspace_name, get_table_holder(k))) {
                    sel.add(k, std::move(n));
                }
            }
        }
        slogger.debug("Affected tables for keyspace {}: {}", keyspace_name, sel.tables);
    }

    // current state of the schema
    auto&& old_keyspaces = co_await read_schema_for_keyspaces(proxy, KEYSPACES, keyspaces);
    auto&& old_scylla_keyspaces = co_await read_schema_for_keyspaces(proxy, SCYLLA_KEYSPACES, keyspaces);
    auto&& old_column_families = co_await read_tables_for_keyspaces(proxy, keyspaces, table_kind::table, affected_tables);
    auto&& old_types = co_await read_schema_for_keyspaces(proxy, TYPES, keyspaces);
    auto&& old_views = co_await read_tables_for_keyspaces(proxy, keyspaces, table_kind::view, affected_tables);
    auto old_functions = co_await read_schema_for_keyspaces(proxy, FUNCTIONS, keyspaces);
    auto old_aggregates = co_await read_schema_for_keyspaces(proxy, AGGREGATES, keyspaces);
    auto old_scylla_aggregates = co_await read_schema_for_keyspaces(proxy, SCYLLA_AGGREGATES, keyspaces);

    co_await proxy.local().get_db().local().apply(freeze(mutations), db::no_timeout);

    // with new data applied
    auto&& new_keyspaces = co_await read_schema_for_keyspaces(proxy, KEYSPACES, keyspaces);
    auto&& new_scylla_keyspaces = co_await read_schema_for_keyspaces(proxy, SCYLLA_KEYSPACES, keyspaces);
    auto&& new_column_families = co_await read_tables_for_keyspaces(proxy, keyspaces, table_kind::table, affected_tables);
    auto&& new_types = co_await read_schema_for_keyspaces(proxy, TYPES, keyspaces);
    auto&& new_views = co_await read_tables_for_keyspaces(proxy, keyspaces, table_kind::view, affected_tables);
    auto new_functions = co_await read_schema_for_keyspaces(proxy, FUNCTIONS, keyspaces);
    auto new_aggregates = co_await read_schema_for_keyspaces(proxy, AGGREGATES, keyspaces);
    auto new_scylla_aggregates = co_await read_schema_for_keyspaces(proxy, SCYLLA_AGGREGATES, keyspaces);

    std::set<sstring> keyspaces_to_drop = co_await merge_keyspaces(proxy, std::move(old_keyspaces), std::move(new_keyspaces),
                                                                   std::move(old_scylla_keyspaces), std::move(new_scylla_keyspaces));
    auto types_to_drop = co_await merge_types(proxy, std::move(old_types), std::move(new_types));
    co_await merge_tables_and_views(proxy, sys_ks,
        std::move(old_column_families), std::move(new_column_families),
        std::move(old_views), std::move(new_views), reload, std::move(tablet_hint));
    co_await merge_functions(proxy, std::move(old_functions), std::move(new_functions));
    co_await merge_aggregates(proxy, std::move(old_aggregates), std::move(new_aggregates), std::move(old_scylla_aggregates), std::move(new_scylla_aggregates));
    co_await types_to_drop.drop();

    auto& sharded_db = proxy.local().get_db();
    // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
    for (auto keyspace_to_drop : keyspaces_to_drop) {
        co_await replica::database::drop_keyspace_on_all_shards(sharded_db, keyspace_to_drop);
    }
}

/**
 * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
 * (which also involves fs operations on add/drop ks/cf)
 *
 * @param mutations the schema changes to apply
 *
 * @throws ConfigurationException If one of metadata attributes has invalid value
 * @throws IOException If data was corrupted during transportation or failed to apply fs operations
 */
future<> merge_schema(sharded<db::system_keyspace>& sys_ks, distributed<service::storage_proxy>& proxy, gms::feature_service& feat, std::vector<mutation> mutations, bool reload)
{
    if (this_shard_id() != 0) {
        // mutations must be applied on the owning shard (0).
        co_await smp::submit_to(0, coroutine::lambda([&, fmuts = freeze(mutations)] () mutable -> future<> {
            co_await merge_schema(sys_ks, proxy, feat, co_await unfreeze_gently(fmuts), reload);
        }));
        co_return;
    }
    co_await with_merge_lock([&] () mutable -> future<> {
        co_await do_merge_schema(proxy, sys_ks, std::move(mutations), reload);
        auto version_from_group0 = co_await get_group0_schema_version(sys_ks.local());
        co_await update_schema_version_and_announce(sys_ks, proxy, feat.cluster_schema_features(), version_from_group0);
    });
}

}

}
