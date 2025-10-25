/*
 * Modified by ScyllaDB
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "schema_applier.hh"

#include <memory>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/rpc/rpc_types.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>

#include <boost/range/join.hpp>

#include <fmt/ranges.h>

#include "absl-flat_hash_map.hh"
#include "service/storage_service.hh"
#include "mutation/frozen_mutation.hh"
#include "schema/schema_fwd.hh"
#include "utils/assert.hh"
#include "cdc/log.hh"
#include "cdc/cdc_partitioner.hh"
#include "view_info.hh"
#include "replica/database.hh"
#include "lang/manager.hh"
#include "db/system_keyspace.hh"
#include "compaction/compaction_manager.hh"
#include "cql3/expr/expression.hh"
#include "types/types.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "gms/feature_service.hh"
#include "dht/i_partitioner.hh"
#include "system_keyspace.hh"
#include "query/query-result-set.hh"
#include "query/query-result-writer.hh"
#include "utils/map_difference.hh"
#include <seastar/coroutine/all.hh>
#include "utils/log.hh"
#include "schema/frozen_schema.hh"
#include "system_keyspace.hh"
#include "system_distributed_keyspace.hh"
#include "cql3/query_processor.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_aggregate.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "mutation/async_utils.hh"

namespace db {

namespace schema_tables {

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

table_selector& table_selector::operator+=(table_selector&& o) {
    all_in_keyspace |= o.all_in_keyspace;
    for (auto t : all_table_kinds) {
        tables[t].merge(std::move(o.tables[t]));
    }
    return *this;
}

void table_selector::add(table_kind t, sstring name) {
    tables[t].emplace(std::move(name));
}

void table_selector::add(sstring name) {
    for (auto t : all_table_kinds) {
        add(t, name);
    }
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

static std::optional<table_id> table_id_from_mutations(const schema_mutations& sm) {
    auto table_rs = query::result_set(sm.columnfamilies_mutation());
    if (table_rs.empty()) {
        return std::nullopt;
    }
    const query::result_set_row& table_row = table_rs.row(0);
    return table_id(table_row.get_nonnull<utils::UUID>("id"));
}

static
future<std::map<table_id, schema_mutations>>
read_tables_for_keyspaces(sharded<service::storage_proxy>& proxy, const std::set<sstring>& keyspace_names, table_kind kind,
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
static read_schema_for_keyspaces(sharded<service::storage_proxy>& proxy, const sstring& schema_table_name, const std::set<sstring>& keyspace_names)
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
static read_table_names_of_keyspace(sharded<service::storage_proxy>& proxy, const sstring& keyspace_name, schema_ptr schema_table) {
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

future<> schema_applier::merge_keyspaces()
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
    auto diff = difference(_before.keyspaces, _after.keyspaces, indirect_equal_to<lw_shared_ptr<query::result_set>>());
    auto sk_diff = difference(_before.scylla_keyspaces, _after.scylla_keyspaces, indirect_equal_to<lw_shared_ptr<query::result_set>>());

    auto created = std::move(diff.entries_only_on_right);
    auto altered = std::move(diff.entries_differing);

    _affected_keyspaces.names.dropped = std::move(diff.entries_only_on_left);

    auto& sk_created = sk_diff.entries_only_on_right;
    auto& sk_altered = sk_diff.entries_differing;
    auto& sk_dropped = sk_diff.entries_only_on_left;

    // For the ALTER case, we have to also consider changes made to SCYLLA_KEYSPACES, not only to KEYSPACES:
    // 1. changes made to non-null columns...
    altered.insert(sk_altered.begin(), sk_altered.end());
    // 2. ... and new or deleted entries - these change only when ALTERing, not CREATE'ing or DROP'ing
    for (auto&& ks : boost::range::join(sk_created, sk_dropped)) {
        if (!created.contains(ks) && !_affected_keyspaces.names.dropped.contains(ks)) {
            altered.emplace(ks);
        }
    }

    auto& sharded_db = _proxy.local().get_db();
    for (auto& name : created) {
        slogger.info("Creating keyspace {}", name);
        auto sk_after_v = _after.scylla_keyspaces.contains(name) ? _after.scylla_keyspaces.at(name) : nullptr;
        auto ksm = co_await create_keyspace_metadata(
                schema_result_value_type{name, _after.keyspaces.at(name)}, sk_after_v);
        _affected_keyspaces.created.push_back(
                co_await replica::database::prepare_create_keyspace_on_all_shards(
                        sharded_db, _proxy, *ksm, _pending_token_metadata));
        _affected_keyspaces.names.created.insert(name);
    }
    for (auto& name : altered) {
        slogger.info("Altering keyspace {}", name);
        auto sk_after_v = _after.scylla_keyspaces.contains(name) ? _after.scylla_keyspaces.at(name) : nullptr;
        auto tmp_ksm = co_await create_keyspace_metadata(
                schema_result_value_type{name, _after.keyspaces.at(name)}, sk_after_v);
        _affected_keyspaces.altered.push_back(
                co_await replica::database::prepare_update_keyspace_on_all_shards(
                        sharded_db, *tmp_ksm, _pending_token_metadata));
        _affected_keyspaces.names.altered.insert(name);
    }
    for (auto& key : _affected_keyspaces.names.dropped) {
        slogger.info("Dropping keyspace {}", key);
    }
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

// see the comments for merge_keyspaces()
future<> schema_applier::merge_types()
{
    auto diff = diff_rows(_before.types, _after.types);
    co_await _affected_user_types.start();
    co_await _affected_user_types.invoke_on_all([&] (affected_user_types_per_shard& af) mutable -> future<> {
        auto& db = _proxy.local().get_db().local();

        std::map<sstring, std::reference_wrapper<replica::keyspace>> new_keyspaces_per_shard;
        for (auto& created_keyspace_per_shard : _affected_keyspaces.created) {
            auto& ks = created_keyspace_per_shard[this_shard_id()];
            new_keyspaces_per_shard.emplace(ks->metadata()->name(), std::ref(*ks));
        }

        af.created = co_await create_types(db, diff.created, new_keyspaces_per_shard);
        af.altered = co_await create_types(db, diff.altered, new_keyspaces_per_shard);
        af.dropped = co_await create_types(db, diff.dropped, new_keyspaces_per_shard);
    });

    co_await _types_storage.init(_proxy.local().get_db(), _affected_keyspaces, _affected_user_types);
}

// Which side of the diff this schema is on?
// Helps ensuring that when creating schema for altered views, we match "before"
// version of view to "before" version of base table and "after" to "after"
// respectively.
enum class schema_diff_side {
    left, // old, before
    right, // new, after
};

static schema_diff_per_shard diff_table_or_view(sharded<service::storage_proxy>& proxy,
    const std::map<table_id, schema_mutations>& before,
    const std::map<table_id, schema_mutations>& after,
    bool reload,
    noncopyable_function<schema_ptr (schema_mutations sm, schema_diff_side)> create_schema)
{
    schema_diff_per_shard d;
    auto diff = difference(before, after);
    for (auto&& key : diff.entries_only_on_left) {
        auto&& s = proxy.local().get_db().local().find_schema(key);
        slogger.info("Dropping {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
        d.dropped.emplace_back(s);
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
        d.altered.emplace_back(schema_diff_per_shard::altered_schema{s_before, s});
    }
    if (reload) {
        for (auto&& key: diff.entries_in_common) {
            auto s = create_schema(std::move(after.at(key)), schema_diff_side::right);
            slogger.info("Reloading {}.{} id={} version={}", s->ks_name(), s->cf_name(), s->id(), s->version());
            d.altered.emplace_back(schema_diff_per_shard::altered_schema {s, s});
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


in_progress_types_storage_per_shard::in_progress_types_storage_per_shard(replica::database& db, const affected_keyspaces& affected_keyspaces, const affected_user_types& affected_types) : _stored_user_types(db.as_user_types_storage()) {
    // initialize metadata for new keyspaces
    for (auto& ks_per_shard : affected_keyspaces.created) {
        auto metadata = ks_per_shard[this_shard_id()]->metadata();
        auto& ks_name = metadata->name();
        if (!_in_progress_types.contains(ks_name)) {
            // copy metadata
            _in_progress_types[ks_name] = metadata->user_types();
        }
    }
    auto& types = affected_types.local();
    // initialize metadata for affected keyspaces (where types change)
    for (auto& type : boost::range::join(boost::range::join(types.created, types.altered), types.dropped)) {
        auto& ks_name = type->_keyspace;
        if (!_in_progress_types.contains(ks_name)) {
            // copy metadata
            _in_progress_types[ks_name] = db.find_keyspace(ks_name).metadata()->user_types();
        }
    }

    for (auto& type : boost::range::join(types.created, types.altered)) {
        auto& ks_name = type->_keyspace;
        _in_progress_types[ks_name].add_type(type);
    }
    for (auto& type : types.dropped) {
        auto& ks_name = type->_keyspace;
        _in_progress_types[ks_name].remove_type(type);
    }
    for (const auto &ks_name : affected_keyspaces.names.dropped) {
        // can't reference a type when it's keyspace is being dropped
        _in_progress_types[ks_name] = data_dictionary::user_types_metadata();
    }
}

const data_dictionary::user_types_metadata& in_progress_types_storage_per_shard::get(const sstring& ks) const {
    if (_in_progress_types.contains(ks)) {
        return _in_progress_types.at(ks);
    }
    // keyspace is not affected
    return _stored_user_types->get(ks);
}

std::shared_ptr<data_dictionary::user_types_storage> in_progress_types_storage_per_shard::committed_storage() {
    return _stored_user_types;
}

future<> in_progress_types_storage::init(sharded<replica::database>& sharded_db, const affected_keyspaces& affected_keyspaces, const affected_user_types& affected_types) {
    co_await sharded_db.invoke_on_all([&] (replica::database& db) {
        shards[this_shard_id()] = make_foreign(seastar::make_shared<in_progress_types_storage_per_shard>(db, affected_keyspaces, affected_types));
    });
}

in_progress_types_storage_per_shard& in_progress_types_storage::local() {
    return *shards[this_shard_id()];
}

// see the comments for merge_keyspaces()
// Atomically publishes schema changes. In particular, this function ensures
// that when a base schema and a subset of its views are modified together (i.e.,
// upon an alter table or alter type statement), then they are published together
// as well, without any deferring in-between.
future<> schema_applier::merge_tables_and_views()
{
    auto& user_types = _types_storage.local();
    co_await _affected_tables_and_views.tables_and_views.start();

    // diffs bound to current shard
    auto& local_views = _affected_tables_and_views.tables_and_views.local().views;
    auto& local_tables = _affected_tables_and_views.tables_and_views.local().tables;
    auto& local_cdc = _affected_tables_and_views.tables_and_views.local().cdc;

    // Create CDC tables before non-CDC base tables, because we want the base tables with CDC enabled
    // to point to their CDC tables.
    local_cdc = diff_table_or_view(_proxy, _before.cdc, _after.cdc, _reload, [&] (schema_mutations sm, schema_diff_side) {
        return create_table_from_mutations(_proxy, std::move(sm), user_types, nullptr);
    });
    local_tables = diff_table_or_view(_proxy, _before.tables, _after.tables, _reload, [&] (schema_mutations sm, schema_diff_side side) {
        // If the table has CDC enabled, find the CDC schema version and set it in the table schema.
        // If the table is created or altered with CDC enabled, then the CDC
        // table is also created or altered in the same operation, so we can
        // find its schema version in the CDC schemas we created above in
        // local_cdc.
        query::result_set rs(sm.columnfamilies_mutation());
        const query::result_set_row& table_row = rs.row(0);

        auto ks_name = table_row.get_nonnull<sstring>("keyspace_name");
        auto cf_name = table_row.get_nonnull<sstring>("table_name");
        auto cdc_name = cdc::log_name(cf_name);

        schema_ptr cdc_schema; // optional CDC schema of this table

        // we only need to set the cdc schema for created schemas and new altered schemas.
        // old altered schemas that we create here will not be used for generating cdc mutations.
        if (side == schema_diff_side::right) {
            for (const auto& cdc_created : local_cdc.created) {
                const auto& new_cdc_schema = cdc_created;
                if (new_cdc_schema->ks_name() == ks_name && new_cdc_schema->cf_name() == cdc_name) {
                    cdc_schema = new_cdc_schema;
                    break;
                }
            }
            for (const auto& cdc_altered : local_cdc.altered) {
                const auto& new_cdc_schema = cdc_altered.new_schema;
                if (new_cdc_schema->ks_name() == ks_name && new_cdc_schema->cf_name() == cdc_name) {
                    cdc_schema = new_cdc_schema;
                    break;
                }
            }
        }

        return create_table_from_mutations(_proxy, std::move(sm), user_types, cdc_schema);
    });
    local_views = diff_table_or_view(_proxy, _before.views, _after.views, _reload, [&] (schema_mutations sm, schema_diff_side side) {
        // The view schema mutation should be created with reference to the base table schema because we definitely know it by now.
        // If we don't do it we are leaving a window where write commands to this schema are illegal.
        // There are 3 possibilities:
        // 1. The table was altered - in this case we want the view to correspond to this new table schema.
        // 2. The table was just created - the table is guaranteed to be published with the view in that case.
        // 3. The view itself was altered - in that case we already know the base table so we can take it from
        //    the database object.
        query::result_set rs(sm.columnfamilies_mutation());
        const query::result_set_row& view_row = rs.row(0);
        auto ks_name = view_row.get_nonnull<sstring>("keyspace_name");
        auto base_name = view_row.get_nonnull<sstring>("base_table_name");

        schema_ptr base_schema;
        for (auto& altered : local_tables.altered) {
            // Chose the appropriate version of the base table schema: old -> old, new -> new.
            schema_ptr s = side == schema_diff_side::left ? altered.old_schema : altered.new_schema;
            if (s->ks_name() == ks_name && s->cf_name() == base_name) {
                base_schema = s;
                break;
            }
        }
        if (!base_schema) {
            for (auto& s : local_tables.created) {
                if (s.get()->ks_name() == ks_name && s.get()->cf_name() == base_name) {
                    base_schema = s;
                    break;
                }
            }
        }

        if (!base_schema) {
            base_schema = _proxy.local().local_db().find_schema(ks_name, base_name);
        }
        view_ptr vp = create_view_from_mutations(_proxy, std::move(sm), user_types, base_schema);

        // Now when we have a referenced base - sanity check that we're not registering an old view
        // (this could happen when we skip multiple major versions in upgrade, which is unsupported.)
        check_no_legacy_secondary_index_mv_schema(_proxy.local().get_db().local(), vp, base_schema);

        return vp;
    });

    // create schema_ptrs for all shards
    frozen_schema_diff tables_frozen = co_await local_tables.freeze();
    frozen_schema_diff cdc_frozen = co_await local_cdc.freeze();
    frozen_schema_diff views_frozen = co_await local_views.freeze();
    co_await _affected_tables_and_views.tables_and_views.invoke_on_others([this, &tables_frozen, &cdc_frozen, &views_frozen] (affected_tables_and_views_per_shard& tables_and_views) -> future<> {
        auto& db = _proxy.local().get_db().local();
        tables_and_views.tables = co_await schema_diff_per_shard::copy_from(
                db, _types_storage, tables_frozen);
        tables_and_views.cdc = co_await schema_diff_per_shard::copy_from(
                db, _types_storage, cdc_frozen);
        tables_and_views.views = co_await schema_diff_per_shard::copy_from(
                db, _types_storage, views_frozen);
    });

    auto& db = _proxy.local().get_db();
    co_await max_concurrent_for_each(local_views.dropped, max_concurrent, [&db, this] (schema_ptr& dt) -> future<> {
        auto uuid = dt->id();
        _affected_tables_and_views.table_shards.insert({uuid,
                co_await replica::database::prepare_drop_table_on_all_shards(db, uuid)});
    });
    co_await max_concurrent_for_each(local_tables.dropped, max_concurrent, [&db, this] (schema_ptr& dt) -> future<> {
        auto uuid = dt->id();
        _affected_tables_and_views.table_shards.insert({uuid,
                co_await replica::database::prepare_drop_table_on_all_shards(db, uuid)});
    });
    co_await max_concurrent_for_each(local_cdc.dropped, max_concurrent, [&db, this] (schema_ptr& dt) -> future<> {
        auto uuid = dt->id();
        _affected_tables_and_views.table_shards.insert({uuid,
                co_await replica::database::prepare_drop_table_on_all_shards(db, uuid)});
    });
}

future<frozen_schema_diff> schema_diff_per_shard::freeze() const {
    frozen_schema_diff result;
    for (const auto& c : created) {
        result.created.emplace_back(extended_frozen_schema(c));
        co_await coroutine::maybe_yield();
    }
    for (const auto& a : altered) {
        result.altered.push_back(frozen_schema_diff::altered_schema{
            .old_schema = extended_frozen_schema(a.old_schema),
            .new_schema = extended_frozen_schema(a.new_schema),
        });
        co_await coroutine::maybe_yield();
    }
    for (const auto& d : dropped) {
        result.dropped.emplace_back(extended_frozen_schema(d));
        co_await coroutine::maybe_yield();
    }
    co_return result;
}

future<schema_diff_per_shard> schema_diff_per_shard::copy_from(replica::database& db, in_progress_types_storage& types_storage, const frozen_schema_diff& oth) {
    auto uts = std::make_shared<in_progress_types_storage_per_shard>(types_storage.local());
    schema_ctxt ctxt(db.get_config(), uts, db.features(), &db);
    schema_ctxt commited_ctxt(db.get_config(), uts->committed_storage(), db.features(), &db);
    schema_diff_per_shard result;

    for (const auto& c : oth.created) {
        result.created.emplace_back(c.unfreeze(ctxt));
        co_await coroutine::maybe_yield();
    }
    for (const auto& a : oth.altered) {
        result.altered.push_back(schema_diff_per_shard::altered_schema{
            .old_schema = a.old_schema.unfreeze(commited_ctxt),
            .new_schema = a.new_schema.unfreeze(ctxt),
        });
        co_await coroutine::maybe_yield();
    }
    for (const auto& d : oth.dropped) {
        result.dropped.emplace_back(d.unfreeze(commited_ctxt));
        co_await coroutine::maybe_yield();
    }

    co_return result;
}

static future<> notify_tables_and_views(service::migration_notifier& notifier, const affected_tables_and_views& diff) {
    auto it = diff.tables_and_views.local().columns_changed.cbegin();
    auto notify = [&] (auto& r, auto&& f) -> future<> {
        co_await max_concurrent_for_each(r, max_concurrent, std::move(f));
    };

    const auto& tables = diff.tables_and_views.local().tables;
    const auto& cdc = diff.tables_and_views.local().cdc;
    const auto& views = diff.tables_and_views.local().views;

    // View drops are notified first, because a table can only be dropped if its views are already deleted
    co_await notify(views.dropped, [&] (auto&& dt) { return notifier.drop_view(view_ptr(dt)); });
    co_await notify(tables.dropped, [&] (auto&& dt) { return notifier.drop_column_family(dt); });
    co_await notify(cdc.dropped, [&] (auto&& dt) { return notifier.drop_column_family(dt); });
    // Table creations are notified first, in case a view is created right after the table
    co_await notify(tables.created, [&] (auto&& gs) { return notifier.create_column_family(gs); });
    co_await notify(cdc.created, [&] (auto&& gs) { return notifier.create_column_family(gs); });
    co_await notify(views.created, [&] (auto&& gs) { return notifier.create_view(view_ptr(gs)); });
    // Table altering is notified first, in case new base columns appear
    co_await notify(tables.altered, [&] (auto&& altered) { return notifier.update_column_family(altered.new_schema, *it++); });
    co_await notify(cdc.altered, [&] (auto&& altered) { return notifier.update_column_family(altered.new_schema, *it++); });
    co_await notify(views.altered, [&] (auto&& altered) { return notifier.update_view(view_ptr(altered.new_schema), *it++); });
}

static void drop_cached_func(replica::database& db, const query::result_set_row& row) {
    auto language = row.get_nonnull<sstring>("language");
    if (language == "wasm") {
        cql3::functions::function_name name{
            row.get_nonnull<sstring>("keyspace_name"), row.get_nonnull<sstring>("function_name")};
        auto arg_types = read_arg_types(row, name.keyspace, db.user_types());
        db.lang().remove(name, arg_types);
    }
}

future<> schema_applier::merge_functions() {
    auto diff = diff_rows(_before.functions, _after.functions);
    co_await _functions_batch.start();
    co_await _functions_batch.invoke_on_all(coroutine::lambda([&] (cql3::functions::change_batch& batch) -> future<> {
        auto& db = _proxy.local().get_db().local();
        for (const auto& val : diff.created) {
            batch.add_function(co_await create_func(db, *val, _types_storage.local()));
        }
        for (const auto& val : diff.dropped) {
            cql3::functions::function_name name{
                val->get_nonnull<sstring>("keyspace_name"), val->get_nonnull<sstring>("function_name")};
            auto commited_storage = _types_storage.local().committed_storage();
            auto arg_types = read_arg_types(*val, name.keyspace, *commited_storage);
            // as we don't yield between dropping cache and committing batch
            // change there is no window between cache removal and declaration removal
            drop_cached_func(db, *val);
            batch.remove_function(name, arg_types);
        }
        for (const auto& val : diff.altered) {
            drop_cached_func(db, *val);
            batch.replace_function(co_await create_func(db, *val, _types_storage.local()));
        }
    }));
}

future<> schema_applier::merge_aggregates() {
    auto diff = diff_aggregates_rows(_before.aggregates, _after.aggregates, _before.scylla_aggregates, _after.scylla_aggregates);

    co_await _functions_batch.invoke_on_all([&] (cql3::functions::change_batch& batch)-> future<> {
        auto& db = _proxy.local().get_db().local();
        for (const auto& val : diff.created) {
            batch.add_function(create_aggregate(db, *val.first, val.second, batch, _types_storage.local()));
        }
        for (const auto& val : diff.dropped) {
            cql3::functions::function_name name{
                val.first->get_nonnull<sstring>("keyspace_name"), val.first->get_nonnull<sstring>("aggregate_name")};
            auto commited_storage = _types_storage.local().committed_storage();
            auto arg_types = read_arg_types(*val.first, name.keyspace, *commited_storage);
            batch.remove_aggregate(name, arg_types);
        }
        co_return;
    });
}

struct extracted_cdc {
    std::map<table_id, schema_mutations> tables_without_cdc;
    std::map<table_id, schema_mutations> cdc_tables;
};

static extracted_cdc extract_cdc(std::map<table_id, schema_mutations> tables) {
    std::map<table_id, schema_mutations> cdc_tables;

    auto it = tables.begin();
    while (it != tables.end()) {
        if (it->second.partitioner() == cdc::cdc_partitioner::classname) {
            auto node = tables.extract(it++);
            cdc_tables.insert(std::move(node));
        } else {
            ++it;
        }
    }

    return extracted_cdc{std::move(tables), std::move(cdc_tables)};
}

future<schema_persisted_state> schema_applier::get_schema_persisted_state() {
    auto tables_and_cdc = co_await read_tables_for_keyspaces(_proxy, _keyspaces, table_kind::table, _affected_tables);
    auto [tables, cdc] = extract_cdc(std::move(tables_and_cdc));

    schema_persisted_state v{
        .keyspaces = co_await read_schema_for_keyspaces(_proxy, KEYSPACES, _keyspaces),
        .scylla_keyspaces = co_await read_schema_for_keyspaces(_proxy, SCYLLA_KEYSPACES, _keyspaces),
        .tables = std::move(tables),
        .types = co_await read_schema_for_keyspaces(_proxy, TYPES, _keyspaces),
        .views = co_await read_tables_for_keyspaces(_proxy, _keyspaces, table_kind::view, _affected_tables),
        .cdc = std::move(cdc),
        .functions = co_await read_schema_for_keyspaces(_proxy, FUNCTIONS, _keyspaces),
        .aggregates = co_await read_schema_for_keyspaces(_proxy, AGGREGATES, _keyspaces),
        .scylla_aggregates = co_await read_schema_for_keyspaces(_proxy, SCYLLA_AGGREGATES, _keyspaces),
    };
    co_return v;
}

future<> schema_applier::prepare(utils::chunked_vector<mutation>& muts) {
    schema_ptr s = keyspaces();
    for (auto& mutation : muts) {
        sstring keyspace_name = value_cast<sstring>(utf8_type->deserialize(mutation.key().get_component(*s, 0)));

        if (schema_tables_holding_schema_mutations().contains(mutation.schema()->id())) {
            _affected_tables[keyspace_name] += get_affected_tables(keyspace_name, mutation);
        }

        replica::update_tablet_metadata_change_hint(_tablet_hint, mutation);

        _keyspaces.emplace(std::move(keyspace_name));
    }

    if (_reload) {
        for (auto&& ks : _proxy.local().get_db().local().get_non_system_keyspaces()) {
            _keyspaces.emplace(ks);
            table_selector sel;
            sel.all_in_keyspace = true;
            _affected_tables[ks] = sel;
        }
    }

    // Resolve sel.all_in_keyspace == true to the actual list of tables and views.
    for (auto&& [keyspace_name, sel] : _affected_tables) {
        if (sel.all_in_keyspace) {
            // FIXME: Obtain from the database object
            slogger.trace("Reading table list for keyspace {}", keyspace_name);
            for (auto k : all_table_kinds) {
                for (auto&& n : co_await read_table_names_of_keyspace(_proxy, keyspace_name, get_table_holder(k))) {
                    sel.add(k, std::move(n));
                }
            }
        }
        slogger.debug("Affected tables for keyspace {}: {}", keyspace_name, sel.tables);
    }

    _before = co_await get_schema_persisted_state();

    for (auto& mut : muts) {
        // We must force recalculation of schema version after the merge, since the resulting
        // schema may be a mix of the old and new schemas, with the exception of entries
        // that originate from group 0.
        maybe_delete_schema_version(mut);
    }
}

class pending_schema_getter : public service::schema_getter {
private:
    schema_applier& _sa;
    sharded<replica::database>& _db;
public:
    pending_schema_getter(schema_applier& sa) :
            _sa(sa), _db(sa._proxy.local().get_db()) {
    };

    virtual flat_hash_map<sstring, locator::replication_strategy_ptr> get_keyspaces_replication() const override {
        flat_hash_map<sstring, locator::replication_strategy_ptr> out;
        for (auto& [name, ks] : _db.local().get_keyspaces()) {
            out.emplace(name, ks.get_replication_strategy_ptr());
        }
        for (const auto& name : _sa._affected_keyspaces.names.dropped) {
            out.erase(name);
        }
        for (const auto& ks_all_shards : _sa._affected_keyspaces.altered) {
            auto& kc = *ks_all_shards[this_shard_id()];
            out.insert_or_assign(kc.metadata->name(), kc.strategy);
        }
        for (const auto& ks_all_shards : _sa._affected_keyspaces.created) {
            auto& ks = *ks_all_shards[this_shard_id()];
            auto name = ks.metadata()->name();
            out.insert_or_assign(name, ks.get_replication_strategy_ptr());
        }
        return out;
    };

    virtual future<> for_each_table_schema_gently(std::function<future<>(table_id, schema_ptr)> f) const override {
        flat_hash_map<table_id, schema_ptr> table_schemas;

        auto ff = [&table_schemas](table_id id, lw_shared_ptr<replica::table> t) -> future<> {
            table_schemas.insert_or_assign(id, t->schema());
            co_return;
        };
        co_await _db.local().get_tables_metadata().for_each_table_gently(ff);

        auto include_pending_changes = [&table_schemas](schema_diff_per_shard d) -> future<> {
            for (auto& schema : d.dropped) {
                co_await maybe_yield();
                table_schemas.erase(schema->id());
            }
            for (auto& change : d.altered) {
                co_await maybe_yield();
                table_schemas.insert_or_assign(change.new_schema->id(), change.new_schema);
            }
            for (auto& schema : d.created) {
                co_await maybe_yield();
                table_schemas.insert_or_assign(schema->id(), schema);
            }
        };
        auto& tables_and_views = _sa._affected_tables_and_views.tables_and_views.local();
        co_await include_pending_changes(tables_and_views.tables);
        co_await include_pending_changes(tables_and_views.cdc);
        co_await include_pending_changes(tables_and_views.views);

        for (auto& [id, schema] : table_schemas) {
            co_await f(id, schema);
        }
    };
};

future<> schema_applier::update_tablets() {
    // Has to be after tablets and views change is prepared
    // otherwise this preparation will generate orphaned erms.
    if (_tablet_hint) {
        slogger.info("Tablet metadata changed");
        pending_schema_getter getter{*this};
        _token_metadata_change = co_await _ss.local().prepare_token_metadata_change(
                _pending_token_metadata.local(), getter);
    }
}

// Loads metadata into a single source of truth to ensure consistency.
// It will be committed later if required by the current schema change.
future<> schema_applier::load_mutable_token_metadata() {
    locator::mutable_token_metadata_ptr current_token_metadata = co_await _ss.local().get_mutable_token_metadata_ptr();
    if (_tablet_hint) {
        auto new_token_metadata = co_await _ss.local().prepare_tablet_metadata(
                _tablet_hint, current_token_metadata);
        co_return co_await _pending_token_metadata.assign(new_token_metadata);
    }
    co_await _pending_token_metadata.assign(current_token_metadata);
}

future<> schema_applier::update() {
    _after = co_await get_schema_persisted_state();
    co_await load_mutable_token_metadata();
    co_await merge_keyspaces();
    co_await merge_types();
    co_await merge_tables_and_views();
    co_await update_tablets();
    co_await merge_functions();
    co_await merge_aggregates();
}

void schema_applier::commit_tables_and_views() {
    auto& sharded_db = _proxy.local().get_db();
    auto& db = sharded_db.local();
    auto& diff = _affected_tables_and_views;
    const auto& tables = diff.tables_and_views.local().tables;
    const auto& cdc = diff.tables_and_views.local().cdc;
    const auto& views = diff.tables_and_views.local().views;

    for (auto& dropped_view : views.dropped) {
        auto s = dropped_view.get();
        replica::database::drop_table(sharded_db, s->ks_name(), s->cf_name(), true, diff.table_shards[s->id()]);
    }
    for (auto& dropped_table : tables.dropped) {
        auto s = dropped_table.get();
        replica::database::drop_table(sharded_db, s->ks_name(), s->cf_name(), true, diff.table_shards[s->id()]);
    }
    for (auto& dropped_cdc : cdc.dropped) {
        auto s = dropped_cdc.get();
        replica::database::drop_table(sharded_db, s->ks_name(), s->cf_name(), true, diff.table_shards[s->id()]);
    }

    for (auto& schema : cdc.created) {
        auto& ks = db.find_keyspace(schema->ks_name());
        db.add_column_family(ks, schema, ks.make_column_family_config(*schema, db), replica::database::is_new_cf::yes, _pending_token_metadata.local());
    }

    for (auto& schema : tables.created) {
        auto& ks = db.find_keyspace(schema->ks_name());
        db.add_column_family(ks, schema, ks.make_column_family_config(*schema, db), replica::database::is_new_cf::yes, _pending_token_metadata.local());
    }

    for (auto& schema : views.created) {
        auto& ks = db.find_keyspace(schema->ks_name());
        db.add_column_family(ks, schema, ks.make_column_family_config(*schema, db), replica::database::is_new_cf::yes, _pending_token_metadata.local());
    }

    diff.tables_and_views.local().columns_changed.reserve(tables.altered.size() + cdc.altered.size() + views.altered.size());
    for (auto&& altered : cdc.altered) {
        bool changed = db.update_column_family(altered.new_schema);
        diff.tables_and_views.local().columns_changed.push_back(changed);
    }
    for (auto&& altered : boost::range::join(tables.altered, views.altered)) {
        bool changed = db.update_column_family(altered.new_schema);
        diff.tables_and_views.local().columns_changed.push_back(changed);
    }
}

void schema_applier::commit_on_shard(replica::database& db) {
    // commit keyspace operations
    for (auto& ks_per_shard : _affected_keyspaces.created) {
        auto ks = ks_per_shard[this_shard_id()].release();
        db.insert_keyspace(std::move(ks));
    }
    for (auto& ks_change_per_shard : _affected_keyspaces.altered) {
        auto ks_change = ks_change_per_shard[this_shard_id()].release();
        db.update_keyspace(std::move(ks_change));
    }

    // commit user defined types,
    // create and update user types before any tables/views are created that potentially
    // use those types
    for (auto& user_type : _affected_user_types.local().created) {
        db.find_keyspace(user_type->_keyspace).add_user_type(user_type);
    }
    for (auto& user_type : _affected_user_types.local().altered) {
        db.find_keyspace(user_type->_keyspace).add_user_type(user_type);
    }

    commit_tables_and_views();

    if (_tablet_hint) {
        _ss.local().commit_token_metadata_change(_token_metadata_change);
    }

    // commit user functions and aggregates
    _functions_batch.local().commit();

    // dropping user types only after tables/views/functions/aggregates that may use some them are dropped
    for (auto& user_type : _affected_user_types.local().dropped) {
        db.find_keyspace(user_type->_keyspace).remove_user_type(user_type);
    }

    // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
    for (const auto& ks_name : _affected_keyspaces.names.dropped) {
        db.drop_keyspace(ks_name);
    }
}

// TODO: move per shard logic directly to raft so that all subsystems can be updated together
// (requires switching all affected subsystems to 'applier' interface first)
future<> schema_applier::commit() {
    auto& sharded_db = _proxy.local().get_db();
    // Adding and dropping tables, or changing tablet metadata, uses this
    // locking mechanism to prevent changes to tables_metadata during preemptive
    // iteration over it (e.g., tables_metadata::for_each_table_gently).
    // However, we can only acquire the (write) lock after preparing all
    // entities for the pending schema change that need to iterate over tables_metadata;
    // otherwise, such iteration would deadlock.
    _metadata_locks = std::make_unique<replica::tables_metadata_lock_on_all_shards>(
            co_await replica::database::lock_tables_metadata(sharded_db));
    // Run func first on shard 0
    // to allow "seeding" of the effective_replication_map
    // with a new e_r_m instance.
    SCYLLA_ASSERT(this_shard_id() == 0);
    commit_on_shard(sharded_db.local());
    co_await sharded_db.invoke_on_others([this] (replica::database& db) {
        commit_on_shard(db);
    });
    // unlock as some functions in post_commit() may read data under those locks
    _metadata_locks = nullptr;
}

future<> schema_applier::finalize_tables_and_views() {
    auto& sharded_db = _proxy.local().get_db();
    auto& diff = _affected_tables_and_views;

    // first drop views and *only then* the tables, if interleaved it can lead
    // to a mv not finding its schema when snapshotting since the main table
    // was already dropped (see https://github.com/scylladb/scylla/issues/5614)
    for (auto& dropped_view : diff.tables_and_views.local().views.dropped) {
        auto s = dropped_view.get();
        co_await replica::database::cleanup_drop_table_on_all_shards(sharded_db, _sys_ks, true, diff.table_shards[s->id()]);
    }
    for (auto& dropped_table : diff.tables_and_views.local().tables.dropped) {
        auto s = dropped_table.get();
        co_await replica::database::cleanup_drop_table_on_all_shards(sharded_db, _sys_ks, true, diff.table_shards[s->id()]);
    }
    for (auto& dropped_cdc : diff.tables_and_views.local().cdc.dropped) {
        auto s = dropped_cdc.get();
        co_await replica::database::cleanup_drop_table_on_all_shards(sharded_db, _sys_ks, true, diff.table_shards[s->id()]);
    }

    if (_tablet_hint) {
        auto& db = sharded_db.local();
        co_await db.get_compaction_manager().get_shared_tombstone_gc_state().
                flush_pending_repair_time_update(db);
        _ss.local().wake_up_topology_state_machine();
    }

    co_await sharded_db.invoke_on_all([&diff] (replica::database& db) -> future<> {
        const auto& tables = diff.tables_and_views.local().tables;
        const auto& cdc = diff.tables_and_views.local().cdc;
        const auto& views = diff.tables_and_views.local().views;
        for (auto& created_cdc : cdc.created) {
            co_await db.make_column_family_directory(created_cdc);
        }
        for (auto& created_table : tables.created) {
            co_await db.make_column_family_directory(created_table);
        }
        for (auto& created_view : views.created) {
            co_await db.make_column_family_directory(created_view);
        }
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
    co_await max_concurrent_for_each(diff.tables_and_views.local().tables.created, max_concurrent, [this] (const schema_ptr& gs) -> future<> {
        co_await store_column_mapping(_proxy, gs, false);
    });
    co_await max_concurrent_for_each(diff.tables_and_views.local().tables.altered, max_concurrent, [this] (const schema_diff_per_shard::altered_schema& altered) -> future<> {
        co_await when_all_succeed(
            store_column_mapping(_proxy, altered.old_schema, true),
            store_column_mapping(_proxy, altered.new_schema, false));
    });
    co_await max_concurrent_for_each(diff.tables_and_views.local().tables.dropped, max_concurrent, [this] (const schema_ptr& s) -> future<> {
        co_await drop_column_mapping(_sys_ks.local(), s->id(), s->version());
    });
}

future<> schema_applier::post_commit() {
    co_await finalize_tables_and_views();
    auto& sharded_db = _proxy.local().get_db();
    co_await sharded_db.invoke_on_all([&] (replica::database& db) -> future<> {
        auto& notifier = db.get_notifier();
        // notify about keyspaces
        for (const auto& name : _affected_keyspaces.names.created) {
            co_await notifier.create_keyspace(name);
        }
        for (const auto& name : _affected_keyspaces.names.altered) {
            co_await notifier.update_keyspace(name);
        }
        for (const auto& name : _affected_keyspaces.names.dropped) {
            co_await notifier.drop_keyspace(name);
        }
        // notify about user types
        auto& types = _affected_user_types.local();
        for (auto& type : types.created) {
            co_await notifier.create_user_type(type);
        }
        for (auto& type : types.altered) {
            co_await notifier.update_user_type(type);
        }
        for (auto& type : types.dropped) {
            co_await notifier.drop_user_type(type);
        }

        co_await notify_tables_and_views(notifier, _affected_tables_and_views);

        // notify about user functions and aggregates
        for (const auto& func : _functions_batch.local().removed_functions) {
            if (func.aggregate) {
                co_await notifier.drop_aggregate(func.name, func.arg_types);
            } else {
                co_await notifier.drop_function(func.name, func.arg_types);
            }
        }
    });
    co_return;
}

future<> schema_applier::destroy() {
    co_await _affected_user_types.stop();
    co_await _affected_tables_and_views.tables_and_views.stop();
    co_await _pending_token_metadata.destroy();
    co_await _token_metadata_change.destroy();
    co_await _functions_batch.stop();
}

static future<> execute_do_merge_schema(sharded<service::storage_proxy>& proxy, schema_applier& ap, utils::chunked_vector<mutation> mutations) {
    co_await ap.prepare(mutations);
    co_await proxy.local().get_db().local().apply(freeze(mutations), db::no_timeout);
    co_await ap.update();
    try {
        co_await ap.commit();
    } catch (...) {
        // We have no good way to recover from partial commit and continuing
        // would mean that schema is in inconsistent state.
        on_fatal_internal_error(slogger, format("schema commit failed: {}", std::current_exception()));
    }
    co_await ap.post_commit();
}

static future<> do_merge_schema(sharded<service::storage_proxy>& proxy,  sharded<service::storage_service>& ss, sharded<db::system_keyspace>& sys_ks, utils::chunked_vector<mutation> mutations, bool reload)
{
    slogger.trace("do_merge_schema: {}", mutations);
    schema_applier ap(proxy, ss, sys_ks, reload);
    std::exception_ptr ex;
    try {
        co_await execute_do_merge_schema(proxy, ap, std::move(mutations));
    } catch (...) {
        ex = std::current_exception();
    }
    co_await ap.destroy();
    if (ex) {
        throw ex;
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
future<> merge_schema(sharded<db::system_keyspace>& sys_ks, sharded<service::storage_proxy>& proxy, sharded<service::storage_service>& ss, gms::feature_service& feat, utils::chunked_vector<mutation> mutations, bool reload)
{
    if (this_shard_id() != 0) {
        // mutations must be applied on the owning shard (0).
        co_await smp::submit_to(0, coroutine::lambda([&, fmuts = freeze(mutations)] () mutable -> future<> {
            co_await merge_schema(sys_ks, proxy, ss, feat, co_await unfreeze_gently(fmuts), reload);
        }));
        co_return;
    }
    co_await with_merge_lock([&] () mutable -> future<> {
        co_await do_merge_schema(proxy, ss, sys_ks, std::move(mutations), reload);
        auto version_from_group0 = co_await get_group0_schema_version(sys_ks.local());
        co_await update_schema_version_and_announce(sys_ks, proxy, feat.cluster_schema_features(), version_from_group0);
    });
}

}

}
