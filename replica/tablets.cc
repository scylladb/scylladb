/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "types/types.hh"
#include "types/tuple.hh"
#include "types/list.hh"
#include "db/system_keyspace.hh"
#include "schema/schema_builder.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/stats.hh"
#include "replica/database.hh"
#include "replica/tablets.hh"
#include "replica/tablet_mutation_builder.hh"

namespace replica {

using namespace locator;

static thread_local auto replica_type = tuple_type_impl::get_instance({uuid_type, int32_type});
static thread_local auto replica_set_type = list_type_impl::get_instance(replica_type, false);

data_type get_replica_set_type() {
    return replica_set_type;
}

schema_ptr make_tablets_schema() {
    // FIXME: Allow UDTs in system keyspace:
    // CREATE TYPE tablet_replica (replica_id uuid, shard int);
    // replica_set_type = frozen<list<tablet_replica>>
    auto id = generate_legacy_id(db::system_keyspace::NAME, db::system_keyspace::TABLETS);
    return schema_builder(db::system_keyspace::NAME, db::system_keyspace::TABLETS, id)
            .with_column("keyspace_name", utf8_type, column_kind::partition_key)
            .with_column("table_id", uuid_type, column_kind::partition_key)
            .with_column("tablet_count", int32_type, column_kind::static_column)
            .with_column("table_name", utf8_type, column_kind::static_column)
            .with_column("last_token", long_type, column_kind::clustering_key)
            .with_column("replicas", replica_set_type)
            .with_column("new_replicas", replica_set_type)
            .with_column("stage", utf8_type)
            .with_version(db::system_keyspace::generate_schema_version(id))
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

future<mutation>
tablet_map_to_mutation(const tablet_map& tablets, table_id id, const sstring& keyspace_name, const sstring& table_name,
                       api::timestamp_type ts) {
    auto s = db::system_keyspace::tablets();
    auto gc_now = gc_clock::now();
    auto tombstone_ts = ts - 1;

    mutation m(s, partition_key::from_exploded(*s, {
        data_value(keyspace_name).serialize_nonnull(),
        data_value(id.uuid()).serialize_nonnull()
    }));
    m.partition().apply(tombstone(tombstone_ts, gc_now));
    m.set_static_cell("tablet_count", data_value(int(tablets.tablet_count())), ts);
    m.set_static_cell("table_name", data_value(table_name), ts);

    tablet_id tid = tablets.first_tablet();
    for (auto&& tablet : tablets.tablets()) {
        auto last_token = tablets.get_last_token(tid);
        auto ck = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(last_token)).serialize_nonnull());
        m.set_clustered_cell(ck, "replicas", make_list_value(replica_set_type, replicas_to_data_value(tablet.replicas)), ts);
        if (auto tr_info = tablets.get_tablet_transition_info(tid)) {
            m.set_clustered_cell(ck, "stage", tablet_transition_stage_to_string(tr_info->stage), ts);
            m.set_clustered_cell(ck, "new_replicas", make_list_value(replica_set_type, replicas_to_data_value(tr_info->next)), ts);
        }
        tid = *tablets.next_tablet(tid);
        co_await coroutine::maybe_yield();
    }
    co_return std::move(m);
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
tablet_mutation_builder::del_transition(dht::token last_token) {
    auto ck = get_ck(last_token);
    auto stage_col = _s->get_column_definition("stage");
    _m.set_clustered_cell(ck, *stage_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    auto new_replicas_col = _s->get_column_definition("new_replicas");
    _m.set_clustered_cell(ck, *new_replicas_col, atomic_cell::make_dead(_ts, gc_clock::now()));
    return *this;
}

mutation make_drop_tablet_map_mutation(const sstring& keyspace_name, table_id id, api::timestamp_type ts) {
    auto s = db::system_keyspace::tablets();
    mutation m(s, partition_key::from_exploded(*s, {
        data_value(keyspace_name).serialize_nonnull(),
        data_value(id.uuid()).serialize_nonnull()
    }));
    m.partition().apply(tombstone(ts, gc_clock::now()));
    return m;
}

static
tablet_replica_set deserialize_replica_set(cql3::untyped_result_set_row::view_type raw_value) {
    tablet_replica_set result;
    auto v = value_cast<list_type_impl::native_type>(
            replica_set_type->deserialize_value(raw_value));
    result.reserve(v.size());
    for (const data_value& replica_v : v) {
        std::vector<data_value> replica_dv = value_cast<tuple_type_impl::native_type>(replica_v);
        result.emplace_back(tablet_replica {
            host_id(value_cast<utils::UUID>(replica_dv[0])),
            shard_id(value_cast<int>(replica_dv[1]))
        });
    }
    return result;
}

future<> save_tablet_metadata(replica::database& db, const tablet_metadata& tm, api::timestamp_type ts) {
    tablet_logger.trace("Saving tablet metadata: {}", tm);
    std::vector<mutation> muts;
    muts.reserve(tm.all_tables().size());
    for (auto&& [id, tablets] : tm.all_tables()) {
        // FIXME: Should we ignore missing tables? Currently doesn't matter because this is only used in tests.
        auto s = db.find_schema(id);
        muts.emplace_back(
                co_await tablet_map_to_mutation(*tablets, id, s->ks_name(), s->cf_name(), ts));
    }
    co_await db.apply(freeze(muts), db::no_timeout);
}

static std::tuple<sstring, table_id> to_tablet_metadata_key(const schema& s, const partition_key& key) {
    const auto elements = key.explode(s);
    auto keyspace_name = value_cast<sstring>(utf8_type->deserialize_value(elements[0]));
    auto table_id = value_cast<utils::UUID>(uuid_type->deserialize_value(elements[1]));
    return std::tuple(std::move(keyspace_name), ::table_id(table_id));
}

static dht::token to_tablet_metadata_row_key(const schema& s, const clustering_key& key) {
    const auto elements = key.explode(s);
    return dht::token::from_int64(value_cast<int64_t>(long_type->deserialize_value(elements[0])));
}

static void do_update_tablet_metadata_change_hint(locator::tablet_metadata_change_hint& hint, const schema& s, const mutation& m) {
    const auto [keyspace_name, table_id] = to_tablet_metadata_key(s, m.key());
    auto it = hint.tables.try_emplace(table_id, locator::tablet_metadata_change_hint::table_hint{keyspace_name, table_id, {}}).first;

    const auto& mp = m.partition();
    if (mp.partition_tombstone() || !mp.row_tombstones().empty()) {
        // If there is a partition tombstone or range tombstone, update the
        // entire partition.
        return;
    }

    auto& tokens = it->second.tokens;
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

locator::tablet_metadata_change_hint get_tablet_metadata_change_hint(const std::vector<canonical_mutation>& mutations) {
    auto s = db::system_keyspace::tablets();

    locator::tablet_metadata_change_hint hint;
    hint.tables.reserve(mutations.size());

    for (const auto& cm : mutations) {
        if (cm.column_family_id() != s->id()) {
            continue;
        }
        do_update_tablet_metadata_change_hint(hint, *s, cm.to_mutation(s));
    }

    return hint;
}

void update_tablet_metadata_change_hint(locator::tablet_metadata_change_hint& hint, const mutation& m) {
    auto s = db::system_keyspace::tablets();
    if (m.column_family_id() != s->id()) {
        return;
    }
    do_update_tablet_metadata_change_hint(hint, *s, m);
}

namespace {

tablet_id process_one_row(table_id table, tablet_map& map, tablet_id tid, const cql3::untyped_result_set_row& row) {
    tablet_replica_set tablet_replicas;
    if (row.has("replicas")) {
        tablet_replicas = deserialize_replica_set(row.get_view("replicas"));
    }

    tablet_replica_set new_tablet_replicas;
    if (row.has("new_replicas")) {
        new_tablet_replicas = deserialize_replica_set(row.get_view("new_replicas"));
    }

    if (row.has("stage")) {
        auto stage = tablet_transition_stage_from_string(row.get_as<sstring>("stage"));

        std::unordered_set<tablet_replica> pending(new_tablet_replicas.begin(), new_tablet_replicas.end());
        for (auto&& r : tablet_replicas) {
            pending.erase(r);
        }
        if (pending.size() > 1) {
            throw std::runtime_error(format("Too many pending replicas for table {} tablet {}: {}",
                                            table, tid, pending));
        }
        if (pending.empty()) {
            throw std::runtime_error(format("No pending replicas for table {} tablet {}, in stage {}",
                                            table, tid, stage));
        }
        map.set_tablet_transition_info(tid, tablet_transition_info{stage,
                std::move(new_tablet_replicas), *pending.begin()});
    }

    map.set_tablet(tid, tablet_info{std::move(tablet_replicas)});

    auto persisted_last_token = dht::token::from_int64(row.get_as<int64_t>("last_token"));
    auto current_last_token = map.get_last_token(tid);
    if (current_last_token != persisted_last_token) {
        tablet_logger.debug("current tablet_map: {}", map);
        throw std::runtime_error(format("last_token mismatch between on-disk ({}) and in-memory ({}) tablet map for table {} tablet {}",
                                        persisted_last_token, current_last_token, table, tid));
    }

    return *map.next_tablet(tid);
}


struct tablet_metadata_builder {
    tablet_metadata& tm;
    struct active_tablet_map {
        table_id table;
        tablet_map map;
        tablet_id tid;
    };
    std::optional<active_tablet_map> current;

    void process_row(const cql3::untyped_result_set_row& row) {
        auto table = table_id(row.get_as<utils::UUID>("table_id"));

        if (!current || current->table != table) {
            if (current) {
                tm.set_tablet_map(current->table, std::move(current->map));
            }
            auto tablet_count = row.get_as<int>("tablet_count");
            auto tmap = tablet_map(tablet_count);
            current = active_tablet_map{table, tmap, tmap.first_tablet()};
        }

        current->tid = process_one_row(current->table, current->map, current->tid, row);
    }

    void on_end_of_stream() {
        if (current) {
            tm.set_tablet_map(current->table, std::move(current->map));
        }
    }
};

} // anonymous namespace

future<tablet_metadata> read_tablet_metadata(cql3::query_processor& qp) {
    tablet_metadata tm;
    tablet_metadata_builder builder{tm};
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
    builder.on_end_of_stream();
    tablet_logger.trace("Read tablet metadata: {}", tm);
    co_return std::move(tm);
}

static future<>
do_update_tablet_metadata_partition(cql3::query_processor& qp, tablet_metadata& tm, const tablet_metadata_change_hint::table_hint& hint) {
    tablet_metadata_builder builder{tm};
    co_await qp.query_internal(
            "select * from system.tablets where keyspace_name = ? and table_id = ?",
            db::consistency_level::ONE,
            {data_value(hint.keyspace_name), data_value(hint.table_id.uuid())},
            1000,
            [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
                builder.process_row(row);
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
    if (builder.current) {
        tm.set_tablet_map(builder.current->table, std::move(builder.current->map));
    } else {
        tm.drop_tablet_map(hint.table_id);
    }
}

static future<>
do_update_tablet_metadata_rows(cql3::query_processor& qp, tablet_map& tmap, const tablet_metadata_change_hint::table_hint& hint) {
    for (const auto token : hint.tokens) {
        auto res = co_await qp.execute_internal(
                "select * from system.tablets where keyspace_name = ? and table_id = ? and last_token = ?",
                db::consistency_level::ONE,
                {data_value(hint.keyspace_name), data_value(hint.table_id.uuid()), data_value(dht::token::to_int64(token))},
                cql3::query_processor::cache_internal::yes);
        const auto tid = tmap.get_tablet_id(token);
        if (res->empty()) {
            throw std::runtime_error("Failed to update tablet metadata: updated row is empty");
        } else {
            tmap.clear_tablet_transition_info(tid);
            process_one_row(hint.table_id, tmap, tid, res->one());
        }
    }
}

future<> update_tablet_metadata(cql3::query_processor& qp, tablet_metadata& tm, const locator::tablet_metadata_change_hint& hint) {
    try {
        for (const auto& [_, table_hint] : hint.tables) {
            if (table_hint.tokens.empty()) {
                co_await do_update_tablet_metadata_partition(qp, tm, table_hint);
            } else {
                co_await tm.mutate_tablet_map_async(table_hint.table_id, [&] (tablet_map& tmap) -> future<> {
                    co_await do_update_tablet_metadata_rows(qp, tmap, table_hint);
                });
            }
        }
    } catch (...) {
        std::throw_with_nested(std::runtime_error("Failed to read tablet metadata"));
    }
    tablet_logger.trace("Updated tablet metadata: {}", tm);
}

future<std::vector<canonical_mutation>> read_tablet_mutations(seastar::sharded<replica::database>& db) {
    auto s = db::system_keyspace::tablets();
    auto rs = co_await db::system_keyspace::query_mutations(db, db::system_keyspace::NAME, db::system_keyspace::TABLETS);
    std::vector<canonical_mutation> result;
    result.reserve(rs->partitions().size());
    for (auto& p: rs->partitions()) {
        result.emplace_back(canonical_mutation(p.mut().unfreeze(s)));
    }
    co_return std::move(result);
}

}
