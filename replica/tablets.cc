/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <fmt/ranges.h>
#include <seastar/coroutine/maybe_yield.hh>

#include "types/types.hh"
#include "types/tuple.hh"
#include "types/list.hh"
#include "db/system_keyspace.hh"
#include "schema/schema_builder.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/stats.hh"
#include "gms/feature_service.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"
#include "replica/tablet_mutation_builder.hh"
#include "sstables/sstable_set.hh"
#include "dht/token.hh"
#include "mutation/async_utils.hh"
#include "compaction/compaction_manager.hh"

namespace replica {

using namespace locator;

static thread_local auto repair_scheduler_config_type = user_type_impl::get_instance(
        "system", "repair_scheduler_config", {"auto_repair_enabled", "auto_repair_threshold"},
        {boolean_type, long_type}, false);
static thread_local auto tablet_task_info_type = user_type_impl::get_instance(
        "system", "tablet_task_info", {"request_type", "tablet_task_id", "request_time", "sched_nr", "sched_time", "repair_hosts_filter", "repair_dcs_filter"},
        {utf8_type, uuid_type, timestamp_type, long_type, timestamp_type, utf8_type, utf8_type}, false);
static thread_local auto replica_type = tuple_type_impl::get_instance({uuid_type, int32_type});
static thread_local auto replica_set_type = list_type_impl::get_instance(replica_type, false);
static thread_local auto tablet_info_type = tuple_type_impl::get_instance({long_type, long_type, replica_set_type});

data_type get_replica_set_type() {
    return replica_set_type;
}

data_type get_tablet_info_type() {
    return tablet_info_type;
}

void tablet_add_repair_scheduler_user_types(const sstring& ks, replica::database& db) {
    db.find_keyspace(ks).add_user_type(repair_scheduler_config_type);
    db.find_keyspace(ks).add_user_type(tablet_task_info_type);
}

schema_ptr make_tablets_schema() {
    // FIXME: Allow UDTs in system keyspace:
    // CREATE TYPE tablet_replica (replica_id uuid, shard int);
    // replica_set_type = frozen<list<tablet_replica>>
    auto id = generate_legacy_id(db::system_keyspace::NAME, db::system_keyspace::TABLETS);
    // Bump the schema version offset for tablet repair scheduler columns
    return schema_builder(db::system_keyspace::NAME, db::system_keyspace::TABLETS, id)
            .with_column("table_id", uuid_type, column_kind::partition_key)
            .with_column("tablet_count", int32_type, column_kind::static_column)
            .with_column("keyspace_name", utf8_type, column_kind::static_column)
            .with_column("table_name", utf8_type, column_kind::static_column)
            .with_column("last_token", long_type, column_kind::clustering_key)
            .with_column("replicas", replica_set_type)
            .with_column("new_replicas", replica_set_type)
            .with_column("stage", utf8_type)
            .with_column("transition", utf8_type)
            .with_column("session", uuid_type)
            .with_column("resize_type", utf8_type, column_kind::static_column)
            .with_column("resize_seq_number", long_type, column_kind::static_column)
            .with_column("repair_time", timestamp_type)
            .with_column("repair_task_info", tablet_task_info_type)
            .with_column("repair_scheduler_config", repair_scheduler_config_type, column_kind::static_column)
            .with_column("sstables_repaired_at", long_type)
            .with_column("repair_incremental_mode", utf8_type)
            .with_column("migration_task_info", tablet_task_info_type)
            .with_column("resize_task_info", tablet_task_info_type, column_kind::static_column)
            .with_column("base_table", uuid_type, column_kind::static_column)
            .with_hash_version()
            .build();
}

std::vector<data_value> replicas_to_data_value(const tablet_replica_set& replicas) {
    std::vector<data_value> result;
    result.reserve(replicas.size());
    for (auto&& replica : replicas) {
        result.emplace_back(make_tuple_value(replica_type, {
                data_value(utils::UUID(replica.host.uuid())),
                data_value(int(replica.shard))
        }));
    }
    return result;
};

data_value tablet_task_info_to_data_value(const locator::tablet_task_info& info) {
    data_value result = make_user_value(tablet_task_info_type, {
        data_value(locator::tablet_task_type_to_string(info.request_type)),
        data_value(info.tablet_task_id.uuid()),
        data_value(info.request_time),
        data_value(info.sched_nr),
        data_value(info.sched_time),
        data_value(locator::tablet_task_info::serialize_repair_hosts_filter(info.repair_hosts_filter)),
        data_value(locator::tablet_task_info::serialize_repair_dcs_filter(info.repair_dcs_filter)),
    });
    return result;
};

data_value repair_scheduler_config_to_data_value(const locator::repair_scheduler_config& config) {
    data_value result = make_user_value(repair_scheduler_config_type, {
        data_value(config.auto_repair_enabled),
        data_value(int64_t(config.auto_repair_threshold.count())),
    });
    return result;
};

static void add_per_table_static_mutations(mutation& m, const per_table_tablet_map& map, api::timestamp_type ts, const gms::feature_service& features) {
    if (features.tablet_repair_scheduler) {
        m.set_static_cell("repair_scheduler_config", repair_scheduler_config_to_data_value(map.repair_scheduler_config()), ts);
    }
}

static void add_per_table_tablet_mutations(mutation& m, const auto& ck, const per_table_tablet_info& tablet, api::timestamp_type ts, const gms::feature_service& features) {
    if (features.tablet_repair_scheduler) {
        if (tablet.repair_task_info.is_valid()) {
            m.set_clustered_cell(ck, "repair_task_info", tablet_task_info_to_data_value(tablet.repair_task_info), ts);
            if (features.tablet_incremental_repair) {
                m.set_clustered_cell(ck, "repair_incremental_mode", locator::tablet_repair_incremental_mode_to_string(tablet.repair_task_info.repair_incremental_mode), ts);
            }
        }
        if (tablet.repair_time != db_clock::time_point{}) {
            m.set_clustered_cell(ck, "repair_time", data_value(tablet.repair_time), ts);
        }
    }

    if (features.tablet_incremental_repair) {
        m.set_clustered_cell(ck, "sstables_repaired_at", data_value(tablet.sstables_repaired_at), ts);
    }
}

// Based on calibration run measuring 6ms time to freeze
// mutation with 16K tablets (with 9 replicas each) on a
// 3.4GHz amd64 cpu, and twice as much for unfreeze.
// 1K tablets would take around 0.4 ms to freeze and 0.8 ms
// to unfreeze.
constexpr size_t min_tablets_in_mutation = 1024;

future<>
tablet_map_to_mutations(const shared_tablet_map& tablets, const per_table_tablet_map& per_table_map, table_id id, const sstring& keyspace_name, const sstring& table_name,
                       api::timestamp_type ts, const gms::feature_service& features, std::function<future<>(mutation)> process_mutation) {
    auto s = db::system_keyspace::tablets();
    auto gc_now = gc_clock::now();
    auto tombstone_ts = ts - 1;

    auto key = partition_key::from_single_value(*s,
        data_value(id.uuid()).serialize_nonnull()
    );

    auto make_mutation = [&] () {
        mutation m(s, key);
        m.partition().apply(tombstone(tombstone_ts, gc_now));
        return m;
    };

    auto m = make_mutation();
    m.set_static_cell("tablet_count", data_value(int(tablets.tablet_count())), ts);
    m.set_static_cell("keyspace_name", data_value(keyspace_name), ts);
    m.set_static_cell("table_name", data_value(table_name), ts);
    m.set_static_cell("resize_type", data_value(tablets.resize_decision().type_name()), ts);
    m.set_static_cell("resize_seq_number", data_value(int64_t(tablets.resize_decision().sequence_number)), ts);
    if (features.tablet_resize_virtual_task && tablets.resize_task_info().is_valid()) {
        m.set_static_cell("resize_task_info", tablet_task_info_to_data_value(tablets.resize_task_info()), ts);
    }
    add_per_table_static_mutations(m, per_table_map, ts, features);

    tablet_id tid = tablets.first_tablet();
    size_t tablets_in_mutation = 0;
    for (auto&& tablet : tablets.tablets()) {
        if (++tablets_in_mutation >= min_tablets_in_mutation && seastar::need_preempt()) {
            tablets_in_mutation = 0;
            co_await coroutine::maybe_yield();
            co_await process_mutation(std::exchange(m, make_mutation()));
        }
        auto last_token = tablets.get_last_token(tid);
        auto ck = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(last_token)).serialize_nonnull());
        m.set_clustered_cell(ck, "replicas", make_list_value(replica_set_type, replicas_to_data_value(tablet.replicas)), ts);
        if (features.tablet_migration_virtual_task && tablet.migration_task_info.is_valid()) {
            m.set_clustered_cell(ck, "migration_task_info", tablet_task_info_to_data_value(tablet.migration_task_info), ts);
        }

        add_per_table_tablet_mutations(m, ck, per_table_map.get_tablet_info(tid), ts, features);

        if (auto tr_info = tablets.get_tablet_transition_info(tid)) {
            m.set_clustered_cell(ck, "stage", tablet_transition_stage_to_string(tr_info->stage), ts);
            m.set_clustered_cell(ck, "transition", tablet_transition_kind_to_string(tr_info->transition), ts);
            m.set_clustered_cell(ck, "new_replicas", make_list_value(replica_set_type, replicas_to_data_value(tr_info->next)), ts);
            if (tr_info->session_id) {
                m.set_clustered_cell(ck, "session", data_value(tr_info->session_id.uuid()), ts);
            }
        }
        tid = *tablets.next_tablet(tid);
    }
    co_await process_mutation(std::move(m));
}

mutation
colocated_tablet_map_to_mutation(const shared_tablet_map& tablets, const per_table_tablet_map& per_table_map, table_id id, const sstring& keyspace_name, const sstring& table_name,
        table_id base_table, api::timestamp_type ts, const gms::feature_service& features) {
    auto s = db::system_keyspace::tablets();
    auto gc_now = gc_clock::now();
    auto tombstone_ts = ts - 1;

    mutation m(s, partition_key::from_single_value(*s,
        data_value(id.uuid()).serialize_nonnull()
    ));
    m.partition().apply(tombstone(tombstone_ts, gc_now));
    m.set_static_cell("keyspace_name", data_value(keyspace_name), ts);
    m.set_static_cell("table_name", data_value(table_name), ts);
    m.set_static_cell("base_table", data_value(base_table.uuid()), ts);

    add_per_table_static_mutations(m, per_table_map, ts, features);

    for (auto&& [tid, per_table] : per_table_map.tablets()) {
        auto last_token = tablets.get_last_token(tid);
        auto ck = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(last_token)).serialize_nonnull());

        add_per_table_tablet_mutations(m, ck, per_table, ts, features);
    }

    return m;
}

tablet_mutation_builder&
tablet_mutation_builder::set_new_replicas(dht::token last_token, locator::tablet_replica_set replicas) {
    _m.set_clustered_cell(get_ck(last_token), "new_replicas", make_list_value(replica_set_type, replicas_to_data_value(replicas)), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_replicas(dht::token last_token, locator::tablet_replica_set replicas) {
    _m.set_clustered_cell(get_ck(last_token), "replicas", make_list_value(replica_set_type, replicas_to_data_value(replicas)), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_stage(dht::token last_token, locator::tablet_transition_stage stage) {
    _m.set_clustered_cell(get_ck(last_token), "stage", data_value(tablet_transition_stage_to_string(stage)), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_transition(dht::token last_token, locator::tablet_transition_kind kind) {
    _m.set_clustered_cell(get_ck(last_token), "transition", data_value(tablet_transition_kind_to_string(kind)), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_session(dht::token last_token, service::session_id session_id) {
    _m.set_clustered_cell(get_ck(last_token), "session", data_value(session_id.uuid()), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::del_session(dht::token last_token) {
    auto session_col = _s->get_column_definition("session");
    _m.set_clustered_cell(get_ck(last_token), *session_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::del_transition(dht::token last_token) {
    auto ck = get_ck(last_token);
    auto stage_col = _s->get_column_definition("stage");
    _m.set_clustered_cell(ck, *stage_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    auto transition_col = _s->get_column_definition("transition");
    _m.set_clustered_cell(ck, *transition_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    auto new_replicas_col = _s->get_column_definition("new_replicas");
    _m.set_clustered_cell(ck, *new_replicas_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    auto session_col = _s->get_column_definition("session");
    _m.set_clustered_cell(ck, *session_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_resize_decision(locator::resize_decision resize_decision, const gms::feature_service& features) {
    _m.set_static_cell("resize_type", data_value(resize_decision.type_name()), _ts);
    _m.set_static_cell("resize_seq_number", data_value(int64_t(resize_decision.sequence_number)), _ts);
    if (resize_decision.split_or_merge()) {
        auto resize_task_info = std::holds_alternative<resize_decision::split>(resize_decision.way)
            ? locator::tablet_task_info::make_split_request()
            : locator::tablet_task_info::make_merge_request();
        resize_task_info.sched_nr++;
        resize_task_info.sched_time = db_clock::now();
        return set_resize_task_info(std::move(resize_task_info), features);
    } else {
        return del_resize_task_info(features);
    }
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_repair_scheduler_config(locator::repair_scheduler_config config) {
    _m.set_static_cell("repair_scheduler_config", repair_scheduler_config_to_data_value(config), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_repair_time(dht::token last_token, db_clock::time_point repair_time) {
    _m.set_clustered_cell(get_ck(last_token), "repair_time", data_value(repair_time), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_sstables_repair_at(dht::token last_token, int64_t sstables_repaired_at) {
    _m.set_clustered_cell(get_ck(last_token), "sstables_repaired_at", data_value(sstables_repaired_at), _ts);
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_repair_task_info(dht::token last_token, locator::tablet_task_info repair_task_info, const gms::feature_service& features) {
    _m.set_clustered_cell(get_ck(last_token), "repair_task_info", tablet_task_info_to_data_value(repair_task_info), _ts);
    if (features.tablet_incremental_repair) {
        auto mode = locator::tablet_repair_incremental_mode_to_string(repair_task_info.repair_incremental_mode);
        _m.set_clustered_cell(get_ck(last_token), "repair_incremental_mode", data_value(mode), _ts);
    }
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::del_repair_task_info(dht::token last_token, const gms::feature_service& features) {
    auto col = _s->get_column_definition("repair_task_info");
    _m.set_clustered_cell(get_ck(last_token), *col, atomic_cell::make_dead(_ts, gc_clock::now()));
    if (features.tablet_incremental_repair) {
        auto col = _s->get_column_definition("repair_incremental_mode");
        _m.set_clustered_cell(get_ck(last_token), *col, atomic_cell::make_dead(_ts, gc_clock::now()));
    }
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_migration_task_info(dht::token last_token, locator::tablet_task_info migration_task_info, const gms::feature_service& features) {
    if (features.tablet_migration_virtual_task) {
        _m.set_clustered_cell(get_ck(last_token), "migration_task_info", tablet_task_info_to_data_value(migration_task_info), _ts);
    }
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::del_migration_task_info(dht::token last_token, const gms::feature_service& features) {
    if (features.tablet_migration_virtual_task) {
        auto col = _s->get_column_definition("migration_task_info");
        _m.set_clustered_cell(get_ck(last_token), *col, atomic_cell::make_dead(_ts, gc_clock::now()));
    }
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_resize_task_info(locator::tablet_task_info resize_task_info, const gms::feature_service& features) {
    if (features.tablet_resize_virtual_task) {
        _m.set_static_cell("resize_task_info", tablet_task_info_to_data_value(resize_task_info), _ts);
    }
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::del_resize_task_info(const gms::feature_service& features) {
    if (features.tablet_resize_virtual_task) {
        auto col = _s->get_column_definition("resize_task_info");
        _m.set_static_cell(*col, atomic_cell::make_dead(_ts, gc_clock::now()));
    }
    return *this;
}

tablet_mutation_builder&
tablet_mutation_builder::set_base_table(table_id base_table) {
    _m.set_static_cell("base_table", data_value(base_table.uuid()), _ts);
    return *this;
}

mutation make_drop_tablet_map_mutation(table_id id, api::timestamp_type ts) {
    auto s = db::system_keyspace::tablets();
    mutation m(s, partition_key::from_single_value(*s,
        data_value(id.uuid()).serialize_nonnull()
    ));
    m.partition().apply(tombstone(ts, gc_clock::now()));
    return m;
}

tablet_replica_set tablet_replica_set_from_cell(const data_value& v) {
    tablet_replica_set result;
    auto list_v = value_cast<list_type_impl::native_type>(v);
    result.reserve(list_v.size());
    for (const data_value& replica_v : list_v) {
        std::vector<data_value> replica_dv = value_cast<tuple_type_impl::native_type>(replica_v);
        result.emplace_back(
            host_id(value_cast<utils::UUID>(replica_dv[0])),
            shard_id(value_cast<int>(replica_dv[1]))
        );
    }
    return result;
}

static
tablet_replica_set deserialize_replica_set(cql3::untyped_result_set_row::view_type raw_value) {
    return tablet_replica_set_from_cell(
            replica_set_type->deserialize_value(raw_value));
}

locator::tablet_task_info tablet_task_info_from_cell(const data_value& v) {
    std::vector<data_value> dv = value_cast<user_type_impl::native_type>(v);
    auto result = locator::tablet_task_info{
        locator::tablet_task_type_from_string(value_cast<sstring>(dv[0])),
        locator::tablet_task_id(value_cast<utils::UUID>(dv[1])),
        value_cast<db_clock::time_point>(dv[2]),
        value_cast<int64_t>(dv[3]),
        value_cast<db_clock::time_point>(dv[4]),
        locator::tablet_task_info::deserialize_repair_hosts_filter(value_cast<sstring>(dv[5])),
        locator::tablet_task_info::deserialize_repair_dcs_filter(value_cast<sstring>(dv[6])),
        locator::tablet_repair_incremental_mode::disabled,
    };
    return result;
}

static
locator::tablet_task_info deserialize_tablet_task_info(cql3::untyped_result_set_row::view_type raw_value) {
    return tablet_task_info_from_cell(
            tablet_task_info_type->deserialize_value(raw_value));
}

locator::repair_scheduler_config repair_scheduler_config_from_cell(const data_value& v) {
    std::vector<data_value> dv = value_cast<user_type_impl::native_type>(v);
    auto result = locator::repair_scheduler_config{
        value_cast<bool>(dv[0]),
        std::chrono::seconds(value_cast<int64_t>(dv[1])),
    };
    return result;
}

static
locator::repair_scheduler_config deserialize_repair_scheduler_config(cql3::untyped_result_set_row::view_type raw_value) {
    return repair_scheduler_config_from_cell(
            repair_scheduler_config_type->deserialize_value(raw_value));
}

future<> save_tablet_metadata(replica::database& db, const tablet_metadata& tm, api::timestamp_type ts) {
    tablet_logger.trace("Saving tablet metadata: {}", tm);
    utils::chunked_vector<frozen_mutation> muts;
    muts.reserve(tm.all_tables_ungrouped().size());
    for (auto&& [base_id, tables] : tm.all_table_groups()) {
        // FIXME: Should we ignore missing tables? Currently doesn't matter because this is only used in tests.
        const auto& shared_map = tm.get_shared_tablet_map(base_id);
        auto s = db.find_schema(base_id);
        co_await tablet_map_to_mutations(shared_map, tm.get_per_table_tablet_map(base_id), base_id, s->ks_name(), s->cf_name(), ts, db.features(), [&] (mutation m) -> future<> {
            muts.emplace_back(co_await freeze_gently(m));
        });
        for (auto id : tables) {
            if (id != base_id) {
                auto s = db.find_schema(id);
                muts.emplace_back(
                        colocated_tablet_map_to_mutation(shared_map, tm.get_per_table_tablet_map(id), id, s->ks_name(), s->cf_name(), base_id, ts, db.features()));
            }
        }
    }
    co_await db.apply(muts, db::no_timeout);
}

static table_id to_tablet_metadata_key(const schema& s, const partition_key& key) {
    const auto elements = key.explode(s);
    return ::table_id(value_cast<utils::UUID>(uuid_type->deserialize_value(elements.front())));
}

static dht::token to_tablet_metadata_row_key(const schema& s, const clustering_key& key) {
    const auto elements = key.explode(s);
    return dht::token::from_int64(value_cast<int64_t>(long_type->deserialize_value(elements[0])));
}

static void do_update_tablet_metadata_change_hint(locator::tablet_metadata_change_hint& hint, const schema& s, const mutation& m) {
    const auto table_id = to_tablet_metadata_key(s, m.key());
    auto it = hint.tables.try_emplace(table_id, locator::tablet_metadata_change_hint::table_hint{table_id, {}}).first;

    const auto& mp = m.partition();
    auto& tokens = it->second.tokens;

    if (mp.partition_tombstone() || !mp.row_tombstones().empty() || !mp.static_row().empty()) {
        // If there is a partition tombstone, range tombstone or static row,
        // update the entire partition. Also clear any row hints that might be
        // present to force a full read of the partition.
        tokens.clear();
        return;
    }

    for (const auto& row : mp.clustered_rows()) {
        // TODO: we do not handle deletions yet, will revisit when tablet count
        // reduction is worked out.
        if (row.row().deleted_at()) {
            tokens.clear();
            return;
        }
        tokens.push_back(to_tablet_metadata_row_key(s, row.key()));
    }
}

static std::optional<tablet_replica_set> maybe_deserialize_replica_set(const rows_entry& row, const column_definition& cdef) {
    const auto* cell = row.row().cells().find_cell(cdef.id);
    if (!cell) {
        return std::nullopt;
    }
    auto dv = cdef.type->deserialize_value(cell->as_atomic_cell(cdef).value());
    return tablet_replica_set_from_cell(dv);
}

static void do_validate_tablet_metadata_change(const locator::tablet_metadata& tm, const schema& s, const mutation& m) {
    const auto table_id = to_tablet_metadata_key(s, m.key());
    const auto& mp = m.partition();

    if (mp.partition_tombstone() || !mp.row_tombstones().empty() || !mp.static_row().empty()) {
        return;
    }

    auto& r_cdef = *s.get_column_definition("replicas");
    auto& nr_cdef = *s.get_column_definition("new_replicas");

    for (const auto& row : mp.clustered_rows()) {
        if (row.row().deleted_at()) {
            return;
        }

        auto new_replicas = maybe_deserialize_replica_set(row, nr_cdef);
        if (!new_replicas) {
            continue;
        }

        auto token = to_tablet_metadata_row_key(s, row.key());
        auto replicas = maybe_deserialize_replica_set(row, r_cdef);
        if (!replicas) {
            replicas = tm.get_tablet_map(table_id).get_tablet_info(token).replicas();
        }

        std::unordered_set<tablet_replica> pending = substract_sets(*new_replicas, *replicas);
        if (pending.size() > 1) {
            throw std::runtime_error(fmt::format("Too many pending replicas for table {} last_token {}: {}",
                                            table_id, token, pending));
        }
    }
}

std::optional<locator::tablet_metadata_change_hint> get_tablet_metadata_change_hint(const utils::chunked_vector<canonical_mutation>& mutations) {
    tablet_logger.trace("tablet_metadata_change_hint({})", mutations.size());
    auto s = db::system_keyspace::tablets();

    std::optional<locator::tablet_metadata_change_hint> hint;

    for (const auto& cm : mutations) {
        tablet_logger.trace("tablet_metadata_change_hint() {} == {}", cm.column_family_id(), s->id());
        if (cm.column_family_id() != s->id()) {
            continue;
        }
        if (!hint) {
            hint.emplace();
            hint->tables.reserve(mutations.size());
        }
        do_update_tablet_metadata_change_hint(*hint, *s, cm.to_mutation(s));
    }

    return hint;
}

void validate_tablet_metadata_change(const locator::tablet_metadata& tm, const utils::chunked_vector<canonical_mutation>& mutations) {
    auto s = db::system_keyspace::tablets();

    for (const auto& cm : mutations) {
        if (cm.column_family_id() != s->id()) {
            continue;
        }

        do_validate_tablet_metadata_change(tm, *s, cm.to_mutation(s));
    }
}

void update_tablet_metadata_change_hint(locator::tablet_metadata_change_hint& hint, const mutation& m) {
    auto s = db::system_keyspace::tablets();
    if (m.column_family_id() != s->id()) {
        return;
    }
    do_update_tablet_metadata_change_hint(hint, *s, m);
}

namespace {

// When we read the rows of a colocated table, we don't have sufficient information
// to construct the per_table_tablet_map. In particular, we don't know the tablet id
// of the row, but only the tablet's last token. In order to get the tablet id we need
// to have the shared tablet_map which has the mapping from tokens to tablet ids.
// So we read the rows temporarily into the raw_per_table_tablet_map, which we later
// convert to the per_table_tablet_map using the tablet_map.
struct raw_per_table_tablet_map {
    std::unordered_map<dht::token, locator::per_table_tablet_info> tablets;
    locator::repair_scheduler_config repair_scheduler_config;

    void set_tablet(dht::token token, locator::per_table_tablet_info info) {
        tablets.insert_or_assign(token, std::move(info));
    }
};

per_table_tablet_map construct_per_table_map_from_raw(const tablet_map& base_map, raw_per_table_tablet_map raw_map) {
    // transform the key from token to tablet id using the base_map
    per_table_tablet_map result;
    for (auto&& [token, info] : raw_map.tablets) {
        auto tid = base_map.get_tablet_id(token);
        result.set_tablet(tid, std::move(info));
    }
    result.set_repair_scheduler_config(std::move(raw_map.repair_scheduler_config));
    return result;
}

tablet_id process_one_row(table_id table, shared_tablet_map& map, per_table_tablet_map& per_table_map, tablet_id tid, const cql3::untyped_result_set_row& row, std::vector<std::pair<table_id, token>>& update_repair_time) {
    tablet_replica_set tablet_replicas;
    if (row.has("replicas")) {
        tablet_replicas = deserialize_replica_set(row.get_view("replicas"));
    }

    tablet_replica_set new_tablet_replicas;
    if (row.has("new_replicas")) {
        new_tablet_replicas = deserialize_replica_set(row.get_view("new_replicas"));
    }

    db_clock::time_point repair_time;
    if (row.has("repair_time")) {
        repair_time = row.get_as<db_clock::time_point>("repair_time");
        update_repair_time.emplace_back(table, map.get_last_token(tid));
    }

    int64_t sstables_repaired_at = 0;
    if (row.has("sstables_repaired_at")) {
        sstables_repaired_at = row.get_as<int64_t>("sstables_repaired_at");
    }

    locator::tablet_task_info repair_task_info;
    if (row.has("repair_task_info")) {
        repair_task_info = deserialize_tablet_task_info(row.get_view("repair_task_info"));
        if (row.has("repair_incremental_mode")) {
            auto inc = row.get_as<sstring>("repair_incremental_mode");
            repair_task_info.repair_incremental_mode = locator::tablet_repair_incremental_mode_from_string(inc);
        }
    }

    locator::tablet_task_info migration_task_info;
    if (row.has("migration_task_info")) {
        migration_task_info = deserialize_tablet_task_info(row.get_view("migration_task_info"));
    }

    if (row.has("stage")) {
        auto stage = tablet_transition_stage_from_string(row.get_as<sstring>("stage"));
        auto transition = tablet_transition_kind_from_string(row.get_as<sstring>("transition"));

        std::unordered_set<tablet_replica> pending = substract_sets(new_tablet_replicas, tablet_replicas);
        if (pending.size() > 1) {
            throw std::runtime_error(fmt::format("Too many pending replicas for table {} tablet {}: {}",
                                            table, tid, pending));
        }
        std::optional<tablet_replica> pending_replica;
        if (pending.size() != 0) {
            pending_replica = *pending.begin();
        }
        service::session_id session_id;
        if (row.has("session")) {
            session_id = service::session_id(row.get_as<utils::UUID>("session"));
        }
        map.set_tablet_transition_info(tid, tablet_transition_info{stage, transition,
                std::move(new_tablet_replicas), pending_replica, session_id});
    }

    tablet_logger.debug("Set sstables_repaired_at={} table={} tablet={}", sstables_repaired_at, table, tid);
    map.set_tablet(tid, shared_tablet_info{std::move(tablet_replicas), std::move(migration_task_info)});
    per_table_map.set_tablet(tid, per_table_tablet_info(repair_time, std::move(repair_task_info), sstables_repaired_at));

    auto persisted_last_token = dht::token::from_int64(row.get_as<int64_t>("last_token"));
    auto current_last_token = map.get_last_token(tid);
    if (current_last_token != persisted_last_token) {
        tablet_logger.debug("current tablet_map: {}", map);
        throw std::runtime_error(format("last_token mismatch between on-disk ({}) and in-memory ({}) tablet map for table {} tablet {}",
                                        persisted_last_token, current_last_token, table, tid));
    }

    return *map.next_tablet(tid);
}

void process_one_colocated_row(table_id table, raw_per_table_tablet_map& per_table_map, const cql3::untyped_result_set_row& row, std::vector<std::pair<table_id, token>>& update_repair_time) {
    auto token = dht::token::from_int64(row.get_as<int64_t>("last_token"));

    db_clock::time_point repair_time;
    if (row.has("repair_time")) {
        repair_time = row.get_as<db_clock::time_point>("repair_time");
        update_repair_time.emplace_back(table, token);
    }

    int64_t sstables_repaired_at = 0;
    if (row.has("sstables_repaired_at")) {
        sstables_repaired_at = row.get_as<int64_t>("sstables_repaired_at");
    }

    locator::tablet_task_info repair_task_info;
    if (row.has("repair_task_info")) {
        repair_task_info = deserialize_tablet_task_info(row.get_view("repair_task_info"));
    }

    tablet_logger.info("Set sstables_repaired_at={} table={} token={}", sstables_repaired_at, table, token);
    per_table_map.set_tablet(token, per_table_tablet_info(repair_time, std::move(repair_task_info), sstables_repaired_at));
}

struct tablet_metadata_builder {
    tablet_metadata& tm;
    std::vector<std::pair<table_id, token>> update_repair_time;

    struct active_tablet_map {
        struct base_tablet_map {
            shared_tablet_map map;
            per_table_tablet_map per_table_map;
            tablet_id tid;
        };
        struct colocated_tablet_map {
            table_id base_table;
            raw_per_table_tablet_map per_table_map;
        };
        table_id table;
        std::variant<base_tablet_map, colocated_tablet_map> v;
    };
    std::optional<active_tablet_map> current;

    // maps a co-located table to its base table.
    // when reading the tablet metadata of a co-located table, we store it in the map, and we apply
    // all co-located tables in on_end_of_stream. This is because we want to apply all normal tables first,
    // to ensure the base table tablet map is already present when we apply the co-located tables.
    struct colocated_table_info_t {
        table_id base_table;
        raw_per_table_tablet_map raw_map;
    };
    std::unordered_map<table_id, colocated_table_info_t> colocated_table_info;

    void flush_current() {
        if (current) {
            std::visit(overloaded_functor {
                [&] (active_tablet_map::base_tablet_map&& base_map) {
                    tm.set_tablet_map(current->table, std::move(base_map.map), std::move(base_map.per_table_map));
                },
                [&] (active_tablet_map::colocated_tablet_map&& colocated_map) {
                    colocated_table_info[current->table] = {
                        .base_table = colocated_map.base_table,
                        .raw_map = std::move(colocated_map.per_table_map),
                    };
                }
            }, std::move(current->v));
        }
    }

    void process_row(const cql3::untyped_result_set_row& row) {
        auto table = table_id(row.get_as<utils::UUID>("table_id"));

        if (!current || current->table != table) {
            flush_current();

            if (row.has("base_table")) {
                auto base_table = table_id(row.get_as<utils::UUID>("base_table"));
                current = active_tablet_map{table, active_tablet_map::colocated_tablet_map{base_table, raw_per_table_tablet_map()}};
            } else {
                auto tablet_count = row.get_as<int>("tablet_count");
                auto tmap = shared_tablet_map(tablet_count);
                auto first_tablet = tmap.first_tablet();
                current = active_tablet_map{table, active_tablet_map::base_tablet_map{std::move(tmap), per_table_tablet_map(), first_tablet}};
            }

            if (std::holds_alternative<active_tablet_map::base_tablet_map>(current->v)) {
                auto& current_map = std::get<active_tablet_map::base_tablet_map>(current->v).map;
                auto& per_table_map = std::get<active_tablet_map::base_tablet_map>(current->v).per_table_map;

                // Resize decision fields are static columns, so set them only once per table.
                if (row.has("resize_type") && row.has("resize_seq_number")) {
                    auto resize_type_name = row.get_as<sstring>("resize_type");
                    int64_t resize_seq_number = row.get_as<int64_t>("resize_seq_number");

                    locator::resize_decision resize_decision(std::move(resize_type_name), resize_seq_number);
                    current_map.set_resize_decision(std::move(resize_decision));
                }
                if (row.has("resize_task_info")) {
                    current_map.set_resize_task_info(deserialize_tablet_task_info(row.get_view("resize_task_info")));
                }

                if (row.has("repair_scheduler_config")) {
                    auto config = deserialize_repair_scheduler_config(row.get_view("repair_scheduler_config"));
                    per_table_map.set_repair_scheduler_config(std::move(config));
                }
            } else if (std::holds_alternative<active_tablet_map::colocated_tablet_map>(current->v)) {
                auto& colocated_map = std::get<active_tablet_map::colocated_tablet_map>(current->v);

                if (row.has("repair_scheduler_config")) {
                    auto config = deserialize_repair_scheduler_config(row.get_view("repair_scheduler_config"));
                    colocated_map.per_table_map.repair_scheduler_config = std::move(config);
                }
            }
        }

        if (row.has("last_token")) {
            std::visit(overloaded_functor {
                [&] (active_tablet_map::base_tablet_map& base_map) {
                    base_map.tid = process_one_row(current->table, base_map.map, base_map.per_table_map, base_map.tid, row, update_repair_time);
                },
                [&] (active_tablet_map::colocated_tablet_map& colocated_map) {
                    process_one_colocated_row(current->table, colocated_map.per_table_map, row, update_repair_time);
                }
            }, current->v);
        }
    }

    future<> on_end_of_stream(replica::database& db) {
        flush_current();

        // Set co-located tables after setting all other tablet maps to ensure the tablet map
        // of the base table is found.
        for (auto&& [table, cinfo] : colocated_table_info) {
            const auto& base_map = tm.get_tablet_map(cinfo.base_table);
            auto per_table_map = construct_per_table_map_from_raw(base_map, std::move(cinfo.raw_map));
            co_await tm.set_colocated_table(table, cinfo.base_table, std::move(per_table_map));
        }

        for (auto& [table, tablet_token] : update_repair_time) {
            const auto& map = tm.get_tablet_map_view(table);
            auto tid = map.get_tablet_id(tablet_token);
            auto myid = db.get_token_metadata().get_my_id();
            auto range = map.get_token_range(tid);
            auto&& info = map.get_tablet_info(tid);
            auto repair_time = info.repair_time();
            for (auto r : info.replicas()) {
                if (r.host == myid) {
                    auto& gc_state = db.get_compaction_manager().get_shared_tombstone_gc_state();
                    gc_state.insert_pending_repair_time_update(table, range, to_gc_clock(repair_time), r.shard);
                    tablet_logger.debug("Insert pending repair time for tombstone gc: table={} tablet={} range={} repair_time={}",
                            table, tid, range, repair_time);
                    break;
                }
            }
        }
    }
};

} // anonymous namespace

future<tablet_metadata> read_tablet_metadata(cql3::query_processor& qp) {
    tablet_metadata tm;
    tablet_metadata_builder builder{tm};
    tablet_logger.trace("Start reading tablet metadata");
    try {
        co_await qp.query_internal("select * from system.tablets",
           [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
               builder.process_row(row);
               return make_ready_future<stop_iteration>(stop_iteration::no);
           });
    } catch (...) {
        if (builder.current) {
            std::throw_with_nested(std::runtime_error(format("Failed to read tablet metadata for table {}", builder.current->table)));
        } else {
            std::throw_with_nested(std::runtime_error("Failed to read tablet metadata"));
        }
    }
    co_await builder.on_end_of_stream(*qp.db().real_database_ptr());
    tablet_logger.trace("Read tablet metadata: {}", tm);
    co_return std::move(tm);
}

future<std::unordered_set<locator::host_id>> read_required_hosts(cql3::query_processor& qp) {
    std::unordered_set<locator::host_id> hosts;

    auto process_row = [&] (const cql3::untyped_result_set_row& row) {
        tablet_replica_set tablet_replicas;
        if (row.has("replicas")) {
            tablet_replicas = deserialize_replica_set(row.get_view("replicas"));
        }

        for (auto&& r : tablet_replicas) {
            hosts.insert(r.host);
        }

        if (row.has("new_replicas")) {
            tablet_replica_set new_tablet_replicas;
            new_tablet_replicas = deserialize_replica_set(row.get_view("new_replicas"));
            for (auto&& r : new_tablet_replicas) {
                hosts.insert(r.host);
            }
        }
    };

    try {
        co_await qp.query_internal("select replicas, new_replicas from system.tablets",
           [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
               process_row(row);
               return make_ready_future<stop_iteration>(stop_iteration::no);
           });
    } catch (...) {
        std::throw_with_nested(std::runtime_error("Failed to read tablet required hosts"));
    }

    co_return std::move(hosts);
}

static future<>
do_update_tablet_metadata_partition(cql3::query_processor& qp, tablet_metadata& tm, const tablet_metadata_change_hint::table_hint& hint, tablet_metadata_builder& builder) {
    co_await qp.query_internal(
            "select * from system.tablets where table_id = ?",
            db::consistency_level::ONE,
            {data_value(hint.table_id.uuid())},
            1000,
            [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
                builder.process_row(row);
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
    if (builder.current) {
        builder.flush_current();
        builder.current = {};
    } else {
        tm.drop_tablet_map(hint.table_id);
    }
}

static future<>
do_update_tablet_metadata_rows(replica::database& db, cql3::query_processor& qp, shared_tablet_map& tmap, per_table_tablet_map& per_table_tmap, const tablet_metadata_change_hint::table_hint& hint, tablet_metadata_builder& builder) {
    for (const auto token : hint.tokens) {
        auto res = co_await qp.execute_internal(
                "select * from system.tablets where table_id = ? and last_token = ?",
                db::consistency_level::ONE,
                {data_value(hint.table_id.uuid()), data_value(dht::token::to_int64(token))},
                cql3::query_processor::cache_internal::yes);
        const auto tid = tmap.get_tablet_id(token);
        if (res->empty()) {
            throw std::runtime_error("Failed to update tablet metadata: updated row is empty");
        } else {
            tmap.clear_tablet_transition_info(tid);
            process_one_row(hint.table_id, tmap, per_table_tmap, tid, res->one(), builder.update_repair_time);
        }
    }
}

static future<>
do_update_colocated_tablet_metadata_rows(replica::database& db, cql3::query_processor& qp, const shared_tablet_map& tmap, per_table_tablet_map& per_table_tmap, const tablet_metadata_change_hint::table_hint& hint, tablet_metadata_builder& builder) {
    for (const auto token : hint.tokens) {
        auto res = co_await qp.execute_internal(
                "select * from system.tablets where table_id = ? and last_token = ?",
                db::consistency_level::ONE,
                {data_value(hint.table_id.uuid()), data_value(dht::token::to_int64(token))},
                cql3::query_processor::cache_internal::yes);
        const auto tid = tmap.get_tablet_id(token);
        if (res->empty()) {
            throw std::runtime_error("Failed to update tablet metadata: updated row is empty");
        } else {
            raw_per_table_tablet_map raw_per_table_tmap;
            process_one_colocated_row(hint.table_id, raw_per_table_tmap, res->one(), builder.update_repair_time);
            per_table_tmap.set_tablet(tid, raw_per_table_tmap.tablets.at(token));
        }
    }
}

future<> update_tablet_metadata(replica::database& db, cql3::query_processor& qp, tablet_metadata& tm, const locator::tablet_metadata_change_hint& hint) {
    tablet_metadata_builder builder{tm};

    try {
        for (const auto& [_, table_hint] : hint.tables) {
            if (table_hint.tokens.empty()) {
                co_await do_update_tablet_metadata_partition(qp, tm, table_hint, builder);
            } else {
                if (tm.is_base_table(table_hint.table_id)) {
                    co_await tm.mutate_tablet_map_async(table_hint.table_id, [&] (shared_tablet_map& tmap, per_table_tablet_map& per_table_tmap) -> future<> {
                        co_await do_update_tablet_metadata_rows(db, qp, tmap, per_table_tmap, table_hint, builder);
                    });
                } else {
                    co_await tm.mutate_colocated_tablet_map_async(table_hint.table_id, [&] (const shared_tablet_map& tmap, per_table_tablet_map& per_table_tmap) -> future<> {
                        co_await do_update_colocated_tablet_metadata_rows(db, qp, tmap, per_table_tmap, table_hint, builder);
                    });
                }
            }
        }
    } catch (...) {
        std::throw_with_nested(std::runtime_error("Failed to read tablet metadata"));
    }
    co_await builder.on_end_of_stream(db);
    tablet_logger.trace("Updated tablet metadata: {}", tm);
}

future<> read_tablet_mutations(seastar::sharded<replica::database>& db, std::function<void(canonical_mutation)> process_mutation) {
    auto s = db::system_keyspace::tablets();
    auto rs = co_await db::system_keyspace::query_mutations(db, db::system_keyspace::NAME, db::system_keyspace::TABLETS);
    utils::chunked_vector<canonical_mutation> result;
    result.reserve(rs->partitions().size());
    constexpr size_t max_rows = min_tablets_in_mutation;
    for (auto& p: rs->partitions()) {
        co_await unfreeze_and_split_gently(p.mut(), s, max_rows, [&] (mutation m) -> future<> {
            process_mutation(co_await make_canonical_mutation_gently(m));
        });
    }
}

// This sstable set provides access to all the stables in the table, using a snapshot of all
// its tablets/storage_groups compound_sstable_set:s.
// The managed sets cannot be modified through tablet_sstable_set, but only jointly read from, so insert() and erase() are disabled.
class tablet_sstable_set : public sstables::sstable_set_impl {
    schema_ptr _schema;
    locator::shared_tablet_map _tablet_map;
    // Keep a single (compound) sstable_set per tablet/storage_group
    absl::flat_hash_map<size_t, lw_shared_ptr<const sstables::sstable_set>, absl::Hash<size_t>> _sstable_sets;
    // Used when ordering is required for correctness, but hot paths will use flat_hash_map
    // which provides faster lookup time.
    std::set<size_t> _sstable_set_ids;
    size_t _size = 0;
    uint64_t _bytes_on_disk = 0;

public:
    tablet_sstable_set(const tablet_sstable_set& o)
        : _schema(o._schema)
        , _tablet_map(o._tablet_map.clone())
        , _sstable_sets(o._sstable_sets)
        , _sstable_set_ids(o._sstable_set_ids)
        , _size(o._size)
        , _bytes_on_disk(o._bytes_on_disk)
    {}

    tablet_sstable_set(schema_ptr s, const storage_group_manager& sgm, const locator::shared_tablet_map& tmap)
        : _schema(std::move(s))
        , _tablet_map(tmap.tablet_count())
    {
        sgm.for_each_storage_group([this] (size_t id, storage_group& sg) {
            auto set = sg.make_sstable_set();
            _size += set->size();
            _bytes_on_disk += set->bytes_on_disk();
            _sstable_sets[id] = std::move(set);
            _sstable_set_ids.insert(id);
        });
    }

    static lw_shared_ptr<sstables::sstable_set> make(schema_ptr s, const storage_group_manager& sgm, const locator::shared_tablet_map& tmap) {
        return make_lw_shared<sstables::sstable_set>(std::make_unique<tablet_sstable_set>(std::move(s), sgm, tmap));
    }

    const schema_ptr& schema() const noexcept {
        return _schema;
    }

    virtual std::unique_ptr<sstable_set_impl> clone() const override {
        return std::make_unique<tablet_sstable_set>(*this);
    }

    virtual std::vector<sstables::shared_sstable> select(const dht::partition_range& range = query::full_partition_range) const override;
    virtual lw_shared_ptr<const sstable_list> all() const override;
    virtual stop_iteration for_each_sstable_until(std::function<stop_iteration(const sstables::shared_sstable&)> func) const override;
    virtual future<stop_iteration> for_each_sstable_gently_until(std::function<future<stop_iteration>(const sstables::shared_sstable&)> func) const override;
    virtual bool insert(sstables::shared_sstable sst) override;
    virtual bool erase(sstables::shared_sstable sst) override;
    virtual size_t size() const noexcept override {
        return _size;
    }
    virtual uint64_t bytes_on_disk() const noexcept override {
        return _bytes_on_disk;
    }
    virtual selector_and_schema_t make_incremental_selector() const override;

    virtual mutation_reader create_single_key_sstable_reader(
            replica::column_family*,
            schema_ptr,
            reader_permit,
            utils::estimated_histogram&,
            const dht::partition_range&,
            const query::partition_slice&,
            tracing::trace_state_ptr,
            streamed_mutation::forwarding,
            mutation_reader::forwarding,
            const sstables::sstable_predicate&) const override;

    // Will always return an engaged sstable set ptr.
    const lw_shared_ptr<const sstables::sstable_set>& find_sstable_set(size_t i) const {
        auto it = _sstable_sets.find(i);
        if (it == _sstable_sets.end() || !it->second) [[unlikely]] {
            on_internal_error(tablet_logger, format("SSTable set wasn't found for tablet {} of table {}.{}", i, schema()->ks_name(), schema()->cf_name()));
        }
        return it->second;
    }

private:
    size_t group_of(const dht::token& t) const noexcept {
        return _tablet_map.get_tablet_id(t).id;
    }
    dht::token first_token_of(size_t idx) const noexcept {
#ifndef SCYLLA_BUILD_MODE_RELEASE
        if (idx >= _tablet_map.tablet_count()) {
            on_fatal_internal_error(tablet_logger, format("first_token_of: idx={} out of range", idx));
        }
#endif
        return _tablet_map.get_first_token(tablet_id(idx));
    }
    dht::token last_token_of(size_t idx) const noexcept {
#ifndef SCYLLA_BUILD_MODE_RELEASE
        if (idx >= _tablet_map.tablet_count()) {
            on_fatal_internal_error(tablet_logger, format("last_token_of: idx={} out of range", idx));
        }
#endif
        return _tablet_map.get_last_token(tablet_id(idx));
    }
    stop_iteration for_each_sstable_set_until(const dht::partition_range&, std::function<stop_iteration(lw_shared_ptr<const sstables::sstable_set>)>) const;
    future<stop_iteration> for_each_sstable_set_gently_until(const dht::partition_range&, std::function<future<stop_iteration>(lw_shared_ptr<const sstables::sstable_set>)>) const;

    auto subrange(const dht::partition_range& pr) const {
        size_t candidate_start = pr.start() ? group_of(pr.start()->value().token()) : size_t(0);
        size_t candidate_end = pr.end() ? group_of(pr.end()->value().token()) : (_tablet_map.tablet_count() - 1);
        return std::ranges::subrange(_sstable_set_ids.lower_bound(candidate_start), _sstable_set_ids.upper_bound(candidate_end));
    }

    friend class tablet_incremental_selector;
};

lw_shared_ptr<sstables::sstable_set> make_tablet_sstable_set(schema_ptr s, const storage_group_manager& sgm, const locator::shared_tablet_map& tmap) {
    return tablet_sstable_set::make(std::move(s), sgm, tmap);
}

future<std::optional<table_id>> read_base_table(cql3::query_processor& qp, table_id tid) {
    auto rs = co_await qp.execute_internal("select * from system.tablets where table_id = ?",
            {tid.uuid()}, cql3::query_processor::cache_internal::no);
    if (rs->empty() || !rs->front().has("base_table")) {
        co_return std::nullopt;
    }

    co_return table_id(rs->front().get_as<utils::UUID>("base_table"));
}

future<std::optional<tablet_transition_stage>> read_tablet_transition_stage(cql3::query_processor& qp, table_id tid, dht::token last_token) {
    if (auto base_table = co_await read_base_table(qp, tid)) {
        tid = *base_table;
    }
    auto rs = co_await qp.execute_internal("select stage from system.tablets where table_id = ? and last_token = ?",
            {tid.uuid(), dht::token::to_int64(last_token)}, cql3::query_processor::cache_internal::no);
    if (rs->empty() || !rs->one().has("stage")) {
        co_return std::nullopt;
    }

    co_return tablet_transition_stage_from_string(rs->one().get_as<sstring>("stage"));
}

stop_iteration tablet_sstable_set::for_each_sstable_set_until(const dht::partition_range& pr, std::function<stop_iteration(lw_shared_ptr<const sstables::sstable_set>)> func) const {
    for (const auto& i : subrange(pr)) {
        const auto& set = find_sstable_set(i);
        if (func(set) == stop_iteration::yes) {
            return stop_iteration::yes;
        }
    }
    return stop_iteration::no;
}

future<stop_iteration> tablet_sstable_set::for_each_sstable_set_gently_until(const dht::partition_range& pr, std::function<future<stop_iteration>(lw_shared_ptr<const sstables::sstable_set>)> func) const {
    for (const auto& i : subrange(pr)) {
        const auto& set = find_sstable_set(i);
        if (co_await func(set) == stop_iteration::yes) {
            co_return stop_iteration::yes;
        }
    }
    co_return stop_iteration::no;
}

std::vector<sstables::shared_sstable> tablet_sstable_set::select(const dht::partition_range& range) const {
    std::vector<sstables::shared_sstable> ret;
    ret.reserve(size());
    for_each_sstable_set_until(range, [&] (lw_shared_ptr<const sstables::sstable_set> set) {
        auto ssts = set->select(range);
        if (ret.empty()) {
            ret = std::move(ssts);
        } else {
            std::move(ssts.begin(), ssts.end(), std::back_inserter(ret));
        }
        return stop_iteration::no;
    });
    tablet_logger.debug("tablet_sstable_set::select: range={} ret={}", range, ret.size());
    return ret;
}

lw_shared_ptr<const sstable_list> tablet_sstable_set::all() const {
    auto ret = make_lw_shared<sstable_list>();
    ret->reserve(size());
    for_each_sstable_set_until(query::full_partition_range, [&] (lw_shared_ptr<const sstables::sstable_set> set) {
        set->for_each_sstable([&] (const sstables::shared_sstable& sst) {
            ret->insert(sst);
        });
        return stop_iteration::no;
    });
    return ret;
}

stop_iteration tablet_sstable_set::for_each_sstable_until(std::function<stop_iteration(const sstables::shared_sstable&)> func) const {
    return for_each_sstable_set_until(query::full_partition_range, [func = std::move(func)] (lw_shared_ptr<const sstables::sstable_set> set) {
        return set->for_each_sstable_until(func);
    });
}

future<stop_iteration> tablet_sstable_set::for_each_sstable_gently_until(std::function<future<stop_iteration>(const sstables::shared_sstable&)> func) const {
    return for_each_sstable_set_gently_until(query::full_partition_range, [func = std::move(func)] (lw_shared_ptr<const sstables::sstable_set> set) {
        return set->for_each_sstable_gently_until(func);
    });
}

bool tablet_sstable_set::insert(sstables::shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}
bool tablet_sstable_set::erase(sstables::shared_sstable sst) {
    throw_with_backtrace<std::bad_function_call>();
}

class tablet_incremental_selector : public sstables::incremental_selector_impl {
    const tablet_sstable_set& _tset;

    // _cur_set and _cur_selector contain a snapshot
    // for the currently selected compaction_group.
    lw_shared_ptr<const sstables::sstable_set> _cur_set;
    std::optional<sstables::sstable_set::incremental_selector> _cur_selector;
    dht::token _lowest_next_token = dht::maximum_token();

public:
    tablet_incremental_selector(const tablet_sstable_set& tset)
            : _tset(tset)
    {}

    virtual std::tuple<dht::partition_range, std::vector<sstables::shared_sstable>, dht::ring_position_ext> select(const selector_pos& s) override {
        // Always return minimum singular range, such that incremental_selector::select() will always call this function,
        // which in turn will find the next sstable set to select sstables from.
        const dht::partition_range current_range = dht::partition_range::make_singular(dht::ring_position::min());

        // pos must be monotonically increasing in the weak sense
        // but caller can skip to a position outside the current set
        const dht::ring_position_view& pos = s.pos;
        auto token = pos.token();
        auto pr_end = s.range ? dht::ring_position_view::for_range_end(*s.range) : dht::ring_position_view::max();
        // End of stream is reached when pos is past the end of the read range (i.e. exclude tablets
        // that doesn't intersect with the range).
        // We don't want to advance next position when EOS has been reached, such that a fast forward
        // to the next tablet range will work.
        bool eos_reached = dht::ring_position_tri_compare(*_tset.schema(), pos, pr_end) > 0;
        if ((!_cur_set || pos.token() >= _lowest_next_token) && !eos_reached) {
            auto idx = _tset.group_of(token);
            if (_tset._sstable_set_ids.contains(idx)) {
                _cur_set = _tset.find_sstable_set(idx);
            }
            // Set the next token to point to the next engaged storage group.
            // It will be considered later on when the _cur_set is exhausted
            _lowest_next_token = find_lowest_next_token(idx);
        }

        if (!_cur_set) {
            auto lowest_next_position = _lowest_next_token.is_maximum()
                ? dht::ring_position_ext::max()
                : dht::ring_position_ext::starting_at(_lowest_next_token);
            tablet_logger.debug("tablet_incremental_selector {}.{}: select pos={}: returning 0 sstables, next_pos={}",
                    _tset.schema()->ks_name(), _tset.schema()->cf_name(), pos, lowest_next_position);
            return std::make_tuple(std::move(current_range), std::vector<sstables::shared_sstable>{}, lowest_next_position);
        }

        _cur_selector.emplace(_cur_set->make_incremental_selector());

        auto res = _cur_selector->select(s);
        // Return all sstables selected on the requested position from the first matching sstable set.
        // This assumes that the underlying sstable sets are disjoint in their token ranges so
        // only one of them contain any given token.
        auto sstables = std::move(res.sstables);
        // Return the lowest next position, such that this function will be called again to select the
        // lowest next position from the selector which previously returned it.
        // Until the current selector is exhausted. In that case,
        // jump to the next compaction_group sstable set.
        dht::ring_position_ext next_position = res.next_position;
        if (next_position.is_max()) {
            // _cur_selector is exhausted.
            // Return a position starting at `_lowest_next_token`
            // that was calculated for the _cur_set
            // (unless it's already maximum_token in which case we just return next_position == ring_position::max()).
            _cur_set = {};
            _cur_selector.reset();
            if (!_lowest_next_token.is_maximum()) {
                next_position = dht::ring_position_ext::starting_at(_lowest_next_token);
            }
        }

        tablet_logger.debug("tablet_incremental_selector {}.{}: select pos={}: returning {} sstables, next_pos={}",
                _tset.schema()->ks_name(), _tset.schema()->cf_name(), pos, sstables.size(), next_position);
        return std::make_tuple(std::move(current_range), std::move(sstables), std::move(next_position));
    }

private:
    // Find the start token of the first engaged sstable_set
    // starting the search from `current_idx` (exclusive).
    dht::token find_lowest_next_token(size_t current_idx) {
        auto it = _tset._sstable_set_ids.upper_bound(current_idx);
        if (it != _tset._sstable_set_ids.end()) {
            return _tset.first_token_of(*it);
        }
        return dht::maximum_token();
    }
};

mutation_reader
tablet_sstable_set::create_single_key_sstable_reader(
        replica::column_family* cf,
        schema_ptr schema,
        reader_permit permit,
        utils::estimated_histogram& sstable_histogram,
        const dht::partition_range& pr,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        const sstables::sstable_predicate& predicate) const {
    // The singular partition_range start bound must be engaged.
    auto idx = group_of(pr.start()->value().token());
    const auto& set = find_sstable_set(idx);
    return set->create_single_key_sstable_reader(cf, std::move(schema), std::move(permit), sstable_histogram, pr, slice, trace_state, fwd, fwd_mr, predicate);
}

sstables::sstable_set_impl::selector_and_schema_t tablet_sstable_set::make_incremental_selector() const {
    return std::make_tuple(std::make_unique<tablet_incremental_selector>(*this), *_schema);
}

} // namespace replica
