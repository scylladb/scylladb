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
static thread_local auto tablet_info_type = tuple_type_impl::get_instance({long_type, long_type, replica_set_type});

data_type get_replica_set_type() {
    return replica_set_type;
}

data_type get_tablet_info_type() {
    return tablet_info_type;
}

schema_ptr make_tablets_schema() {
    // FIXME: Allow UDTs in system keyspace:
    // CREATE TYPE tablet_replica (replica_id uuid, shard int);
    // replica_set_type = frozen<list<tablet_replica>>
    auto id = generate_legacy_id(db::system_keyspace::NAME, db::system_keyspace::TABLETS);
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

    mutation m(s, partition_key::from_single_value(*s,
        data_value(id.uuid()).serialize_nonnull()
    ));
    m.partition().apply(tombstone(tombstone_ts, gc_now));
    m.set_static_cell("tablet_count", data_value(int(tablets.tablet_count())), ts);
    m.set_static_cell("keyspace_name", data_value(keyspace_name), ts);
    m.set_static_cell("table_name", data_value(table_name), ts);
    m.set_static_cell("resize_type", data_value(tablets.resize_decision().type_name()), ts);
    m.set_static_cell("resize_seq_number", data_value(int64_t(tablets.resize_decision().sequence_number)), ts);

    tablet_id tid = tablets.first_tablet();
    for (auto&& tablet : tablets.tablets()) {
        auto last_token = tablets.get_last_token(tid);
        auto ck = clustering_key::from_single_value(*s, data_value(dht::token::to_int64(last_token)).serialize_nonnull());
        m.set_clustered_cell(ck, "replicas", make_list_value(replica_set_type, replicas_to_data_value(tablet.replicas)), ts);
        if (auto tr_info = tablets.get_tablet_transition_info(tid)) {
            m.set_clustered_cell(ck, "stage", tablet_transition_stage_to_string(tr_info->stage), ts);
            m.set_clustered_cell(ck, "transition", tablet_transition_kind_to_string(tr_info->transition), ts);
            m.set_clustered_cell(ck, "new_replicas", make_list_value(replica_set_type, replicas_to_data_value(tr_info->next)), ts);
            if (tr_info->session_id) {
                m.set_clustered_cell(ck, "session", data_value(tr_info->session_id.uuid()), ts);
            }
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
tablet_mutation_builder::set_resize_decision(locator::resize_decision resize_decision) {
    _m.set_static_cell("resize_type", data_value(resize_decision.type_name()), _ts);
    _m.set_static_cell("resize_seq_number", data_value(int64_t(resize_decision.sequence_number)), _ts);
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
                co_await tablet_map_to_mutation(tablets, id, s->ks_name(), s->cf_name(), ts));
    }
    co_await db.apply(freeze(muts), db::no_timeout);
}

future<tablet_metadata> read_tablet_metadata(cql3::query_processor& qp) {
    tablet_metadata tm;
    struct active_tablet_map {
        table_id table;
        tablet_map map;
        tablet_id tid;
    };
    std::optional<active_tablet_map> current;

    auto process_row = [&] (const cql3::untyped_result_set_row& row) {
        auto table = table_id(row.get_as<utils::UUID>("table_id"));

        if (!current || current->table != table) {
            if (current) {
                tm.set_tablet_map(current->table, std::move(current->map));
            }
            auto tablet_count = row.get_as<int>("tablet_count");
            auto tmap = tablet_map(tablet_count);
            current = active_tablet_map{table, tmap, tmap.first_tablet()};

            // Resize decision fields are static columns, so set them only once per table.
            if (row.has("resize_type") && row.has("resize_seq_number")) {
                auto resize_type_name = row.get_as<sstring>("resize_type");
                int64_t resize_seq_number = row.get_as<int64_t>("resize_seq_number");

                locator::resize_decision resize_decision(std::move(resize_type_name), resize_seq_number);
                current->map.set_resize_decision(std::move(resize_decision));
            }
        }

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
            auto transition = tablet_transition_kind_from_string(row.get_as<sstring>("transition"));

            std::unordered_set<tablet_replica> pending(new_tablet_replicas.begin(), new_tablet_replicas.end());
            for (auto&& r : tablet_replicas) {
                pending.erase(r);
            }
            if (pending.size() == 0) {
                throw std::runtime_error(format("Stage set but no pending replica for table {} tablet {}",
                                                table, current->tid));
            }
            if (pending.size() > 1) {
                throw std::runtime_error(format("Too many pending replicas for table {} tablet {}: {}",
                                                table, current->tid, pending));
            }
            service::session_id session_id;
            if (row.has("session")) {
                session_id = service::session_id(row.get_as<utils::UUID>("session"));
            }
            current->map.set_tablet_transition_info(current->tid, tablet_transition_info{stage, transition,
                    std::move(new_tablet_replicas), *pending.begin(), session_id});
        }

        current->map.set_tablet(current->tid, tablet_info{std::move(tablet_replicas)});

        auto persisted_last_token = dht::token::from_int64(row.get_as<int64_t>("last_token"));
        auto current_last_token = current->map.get_last_token(current->tid);
        if (current_last_token != persisted_last_token) {
            tablet_logger.debug("current tablet_map: {}", current->map);
            throw std::runtime_error(format("last_token mismatch between on-disk ({}) and in-memory ({}) tablet map for table {} tablet {}",
                                            persisted_last_token, current_last_token, table, current->tid));
        }

        current->tid = *current->map.next_tablet(current->tid);
    };

    try {
        co_await qp.query_internal("select * from system.tablets",
           [&] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
               process_row(row);
               return make_ready_future<stop_iteration>(stop_iteration::no);
           });
    } catch (...) {
        if (current) {
            std::throw_with_nested(std::runtime_error(format("Failed to read tablet metadata for table {}", current->table)));
        } else {
            std::throw_with_nested(std::runtime_error("Failed to read tablet metadata"));
        }
    }
    if (current) {
        tm.set_tablet_map(current->table, std::move(current->map));
    }
    tablet_logger.trace("Read tablet metadata: {}", tm);
    co_return std::move(tm);
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
