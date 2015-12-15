/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>

#include "system_keyspace.hh"
#include "types.hh"
#include "service/storage_service.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "service/query_state.hh"
#include "cql3/query_options.hh"
#include "cql3/query_processor.hh"
#include "utils/fb_utilities.hh"
#include "utils/hash.hh"
#include "dht/i_partitioner.hh"
#include "version.hh"
#include "thrift/server.hh"
#include "exceptions/exceptions.hh"
#include "cql3/query_processor.hh"
#include "db/serializer.hh"
#include "query_context.hh"
#include "partition_slice_builder.hh"
#include "db/config.hh"
#include "schema_builder.hh"
#include "md5_hasher.hh"
#include "release.hh"
#include <core/enum.hh>

using days = std::chrono::duration<int, std::ratio<24 * 3600>>;

namespace db {

std::unique_ptr<query_context> qctx = {};

namespace system_keyspace {

static const api::timestamp_type creation_timestamp = api::new_timestamp();

api::timestamp_type schema_creation_timestamp() {
    return creation_timestamp;
}

// Increase whenever changing schema of any system table.
// FIXME: Make automatic by calculating from schema structure.
static const uint16_t version_sequence_number = 1;

table_schema_version generate_schema_version(utils::UUID table_id) {
    md5_hasher h;
    feed_hash(h, table_id);
    feed_hash(h, version_sequence_number);
    return utils::UUID_gen::get_name_UUID(h.finalize());
}

// Currently, the type variables (uuid_type, etc.) are thread-local reference-
// counted shared pointers. This forces us to also make the built in schemas
// below thread-local as well.
// We return schema_ptr, not schema&, because that's the "tradition" in our
// other code.
// We hide the thread_local variable inside a function, because if we later
// we remove the thread_local, we'll start having initialization order
// problems (we need the type variables to be constructed first), and using
// functions will solve this problem. So we use functions right now.

schema_ptr hints() {
    static thread_local auto hints = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, HINTS), NAME, HINTS,
        // partition key
        {{"target_id", uuid_type}},
        // clustering key
        {{"hint_id", timeuuid_type}, {"message_version", int32_type}},
        // regular columns
        {{"mutation", bytes_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "hints awaiting delivery"
       )));
       builder.set_gc_grace_seconds(0);
       builder.set_compaction_strategy_options({{ "enabled", "false" }});
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return hints;
}

schema_ptr batchlog() {
    static thread_local auto batchlog = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, BATCHLOG), NAME, BATCHLOG,
        // partition key
        {{"id", uuid_type}},
        // clustering key
        {},
        // regular columns
        {{"data", bytes_type}, {"version", int32_type}, {"written_at", timestamp_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "batches awaiting replay"
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .compactionStrategyOptions(Collections.singletonMap("min_threshold", "2"))
       )));
       builder.set_gc_grace_seconds(0);
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return batchlog;
}

/*static*/ schema_ptr paxos() {
    static thread_local auto paxos = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, PAXOS), NAME, PAXOS,
        // partition key
        {{"row_key", bytes_type}},
        // clustering key
        {{"cf_id", uuid_type}},
        // regular columns
        {{"in_progress_ballot", timeuuid_type}, {"most_recent_commit", bytes_type}, {"most_recent_commit_at", timeuuid_type}, {"proposal", bytes_type}, {"proposal_ballot", timeuuid_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "in-progress paxos proposals"
        // FIXME: the original Java code also had:
        // operations on resulting CFMetaData:
        //    .compactionStrategyClass(LeveledCompactionStrategy.class);
       )));
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return paxos;
}

schema_ptr built_indexes() {
    static thread_local auto built_indexes = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, BUILT_INDEXES), NAME, BUILT_INDEXES,
        // partition key
        {{"table_name", utf8_type}},
        // clustering key
        {{"index_name", utf8_type}},
        // regular columns
        {},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "built column indexes"
       )));
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::yes);
    }();
    return built_indexes;
}

/*static*/ schema_ptr local() {
    static thread_local auto local = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, LOCAL), NAME, LOCAL,
        // partition key
        {{"key", utf8_type}},
        // clustering key
        {},
        // regular columns
        {
                {"bootstrapped", utf8_type},
                {"cluster_name", utf8_type},
                {"cql_version", utf8_type},
                {"data_center", utf8_type},
                {"gossip_generation", int32_type},
                {"host_id", uuid_type},
                {"native_protocol_version", utf8_type},
                {"partitioner", utf8_type},
                {"rack", utf8_type},
                {"release_version", utf8_type},
                {"schema_version", uuid_type},
                {"thrift_version", utf8_type},
                {"tokens", set_type_impl::get_instance(utf8_type, true)},
                {"truncated_at", map_type_impl::get_instance(uuid_type, bytes_type, true)},
                // The following 3 columns are only present up until 2.1.8 tables
                {"rpc_address", inet_addr_type},
                {"broadcast_address", inet_addr_type},
                {"listen_address", inet_addr_type},

        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about the local node"
       )));
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return local;
}

/*static*/ schema_ptr peers() {
    static thread_local auto peers = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, PEERS), NAME, PEERS,
        // partition key
        {{"peer", inet_addr_type}},
        // clustering key
        {},
        // regular columns
        {
                {"data_center", utf8_type},
                {"host_id", uuid_type},
                {"preferred_ip", inet_addr_type},
                {"rack", utf8_type},
                {"release_version", utf8_type},
                {"rpc_address", inet_addr_type},
                {"schema_version", uuid_type},
                {"tokens", set_type_impl::get_instance(utf8_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "information about known peers in the cluster"
       )));
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return peers;
}

/*static*/ schema_ptr peer_events() {
    static thread_local auto peer_events = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, PEER_EVENTS), NAME, PEER_EVENTS,
        // partition key
        {{"peer", inet_addr_type}},
        // clustering key
        {},
        // regular columns
        {
            {"hints_dropped", map_type_impl::get_instance(uuid_type, int32_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "events related to peers"
       )));
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return peer_events;
}

/*static*/ schema_ptr range_xfers() {
    static thread_local auto range_xfers = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, RANGE_XFERS), NAME, RANGE_XFERS,
        // partition key
        {{"token_bytes", bytes_type}},
        // clustering key
        {},
        // regular columns
        {{"requested_at", timestamp_type}},
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "ranges requested for transfer"
       )));
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return range_xfers;
}

/*static*/ schema_ptr compactions_in_progress() {
    static thread_local auto compactions_in_progress = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, COMPACTIONS_IN_PROGRESS), NAME, COMPACTIONS_IN_PROGRESS,
        // partition key
        {{"id", uuid_type}},
        // clustering key
        {},
        // regular columns
        {
            {"columnfamily_name", utf8_type},
            {"inputs", set_type_impl::get_instance(int32_type, true)},
            {"keyspace_name", utf8_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "unfinished compactions"
        )));
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return compactions_in_progress;
}

/*static*/ schema_ptr compaction_history() {
    static thread_local auto compaction_history = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, COMPACTION_HISTORY), NAME, COMPACTION_HISTORY,
        // partition key
        {{"id", uuid_type}},
        // clustering key
        {},
        // regular columns
        {
            {"bytes_in", long_type},
            {"bytes_out", long_type},
            {"columnfamily_name", utf8_type},
            {"compacted_at", timestamp_type},
            {"keyspace_name", utf8_type},
            {"rows_merged", map_type_impl::get_instance(int32_type, long_type, true)},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "week-long compaction history"
        )));
        builder.set_default_time_to_live(std::chrono::duration_cast<std::chrono::seconds>(days(7)));
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build(schema_builder::compact_storage::no);
    }();
    return compaction_history;
}

/*static*/ schema_ptr sstable_activity() {
    static thread_local auto sstable_activity = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, SSTABLE_ACTIVITY), NAME, SSTABLE_ACTIVITY,
        // partition key
        {
            {"keyspace_name", utf8_type},
            {"columnfamily_name", utf8_type},
            {"generation", int32_type},
        },
        // clustering key
        {},
        // regular columns
        {
            {"rate_120m", double_type},
            {"rate_15m", double_type},
        },
        // static columns
        {},
        // regular column name type
        utf8_type,
        // comment
        "historic sstable read rates"
       )));
       builder.with_version(generate_schema_version(builder.uuid()));
       return builder.build(schema_builder::compact_storage::no);
    }();
    return sstable_activity;
}

schema_ptr size_estimates() {
    static thread_local auto size_estimates = [] {
        schema_builder builder(make_lw_shared(schema(generate_legacy_id(NAME, SIZE_ESTIMATES), NAME, SIZE_ESTIMATES,
            // partition key
            {{"keyspace_name", utf8_type}},
            // clustering key
            {{"table_name", utf8_type}, {"range_start", utf8_type}, {"range_end", utf8_type}},
            // regular columns
            {
                {"mean_partition_size", long_type},
                {"partitions_count", long_type},
            },
            // static columns
            {},
            // regular column name type
            utf8_type,
            // comment
            "per-table primary range size estimates"
            )));
        builder.set_gc_grace_seconds(0);
        builder.with_version(generate_schema_version(builder.uuid()));
        return builder.build(schema_builder::compact_storage::no);
    }();
    return size_estimates;
}

static future<> setup_version() {
    sstring req = "INSERT INTO system.%s (key, release_version, cql_version, thrift_version, native_protocol_version, data_center, rack, partitioner, rpc_address, broadcast_address, listen_address) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    auto& snitch = locator::i_endpoint_snitch::get_local_snitch_ptr();

    return execute_cql(req, db::system_keyspace::LOCAL,
                             sstring(db::system_keyspace::LOCAL),
                             version::release(),
                             cql3::query_processor::CQL_VERSION,
                             org::apache::cassandra::thrift_version,
                             to_sstring(version::native_protocol()),
                             snitch->get_datacenter(utils::fb_utilities::get_broadcast_address()),
                             snitch->get_rack(utils::fb_utilities::get_broadcast_address()),
                             sstring(dht::global_partitioner().name()),
                             gms::inet_address(qctx->db().get_config().rpc_address()).addr(),
                             utils::fb_utilities::get_broadcast_address().addr(),
                             net::get_local_messaging_service().listen_address().addr()
    ).discard_result();
}

future<> check_health();
future<> force_blocking_flush(sstring cfname);

// Changing the real load_dc_rack_info into a future would trigger a tidal wave of futurization that would spread
// even into simple string operations like get_rack() / get_dc(). We will cache those at startup, and then change
// our view of it every time we do updates on those values.
//
// The cache must be distributed, because the values themselves may not update atomically, so a shard reading that
// is different than the one that wrote, may see a corrupted value. invoke_on_all will be used to guarantee that all
// updates are propagated correctly.
struct local_cache {
    std::unordered_map<gms::inet_address, locator::endpoint_dc_rack> _cached_dc_rack_info;
    bootstrap_state _state;
    future<> stop() {
        return make_ready_future<>();
    }
};
static distributed<local_cache> _local_cache;

static future<> build_dc_rack_info() {
    return execute_cql("SELECT peer, data_center, rack from system.%s", PEERS).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        return do_for_each(*msg, [] (auto& row) {
            // Not ideal to assume ipv4 here, but currently this is what the cql types wraps.
            net::ipv4_address peer = row.template get_as<net::ipv4_address>("peer");
            if (!row.has("data_center") || !row.has("rack")) {
                return make_ready_future<>();
            }
            gms::inet_address gms_addr(std::move(peer));
            sstring dc = row.template get_as<sstring>("data_center");
            sstring rack = row.template get_as<sstring>("rack");

            locator::endpoint_dc_rack  element = { dc, rack };
            return _local_cache.invoke_on_all([gms_addr = std::move(gms_addr), element = std::move(element)] (local_cache& lc) {
                lc._cached_dc_rack_info.emplace(gms_addr, element);
            });
        }).then([msg] {
            // Keep msg alive.
        });
    });
}

static future<> build_bootstrap_info() {
    sstring req = "SELECT bootstrapped FROM system.%s WHERE key = ? ";
    return execute_cql(req, LOCAL, sstring(LOCAL)).then([] (auto msg) {
        static auto state_map = std::unordered_map<sstring, bootstrap_state>({
            { "NEEDS_BOOTSTRAP", bootstrap_state::NEEDS_BOOTSTRAP },
            { "COMPLETED", bootstrap_state::COMPLETED },
            { "IN_PROGRESS", bootstrap_state::IN_PROGRESS },
            { "DECOMMISSIONED", bootstrap_state::DECOMMISSIONED }
        });
        bootstrap_state state = bootstrap_state::NEEDS_BOOTSTRAP;

        if (!msg->empty() && msg->one().has("bootstrapped")) {
            state = state_map.at(msg->one().template get_as<sstring>("bootstrapped"));
        }
        return _local_cache.invoke_on_all([state] (local_cache& lc) {
            lc._state = state;
        });
    });
}

future<> init_local_cache() {
    return _local_cache.start().then([] {
        engine().at_exit([] {
            return _local_cache.stop();
        });
    });
}

future<> deinit_local_cache() {
    return _local_cache.stop();
}

void minimal_setup(distributed<database>& db, distributed<cql3::query_processor>& qp) {
    qctx = std::make_unique<query_context>(db, qp);
}

future<> setup(distributed<database>& db, distributed<cql3::query_processor>& qp) {
    minimal_setup(db, qp);
    return setup_version().then([&db] {
        return update_schema_version(db.local().get_version());
    }).then([] {
        return init_local_cache();
    }).then([] {
        return build_dc_rack_info();
    }).then([] {
        return build_bootstrap_info();
    }).then([] {
        return check_health();
    }).then([] {
        return db::schema_tables::save_system_keyspace_schema();
    }).then([] {
        return net::get_messaging_service().invoke_on_all([] (auto& ms){
            return ms.init_local_preferred_ip_cache();
        });
    });
}

typedef std::pair<replay_positions, db_clock::time_point> truncation_entry;
typedef utils::UUID truncation_key;
typedef std::unordered_map<truncation_key, truncation_entry> truncation_map;

static thread_local std::experimental::optional<truncation_map> truncation_records;

future<> save_truncation_records(const column_family& cf, db_clock::time_point truncated_at, replay_positions positions) {
    auto size =
            sizeof(db_clock::rep)
                    + positions.size()
                            * db::serializer<replay_position>(
                                    db::replay_position()).size();
    bytes buf(bytes::initialized_later(), size);
    data_output out(buf);

    // Old version would write a single RP. We write N. Resulting blob size
    // will determine how many.
    // An external entity reading this blob would get a "correct" RP
    // and a garbled time stamp. But an external entity has no business
    // reading this data anyway, since it is meaningless outside this
    // machine instance.
    for (auto& rp : positions) {
        db::serializer<replay_position>::write(out, rp);
    }
    out.write<db_clock::rep>(truncated_at.time_since_epoch().count());

    map_type_impl::native_type tmp;
    tmp.emplace_back(cf.schema()->id(), data_value(buf));
    auto map_type = map_type_impl::get_instance(uuid_type, bytes_type, true);

    sstring req = sprint("UPDATE system.%s SET truncated_at = truncated_at + ? WHERE key = '%s'", LOCAL, LOCAL);
    return qctx->qp().execute_internal(req, {make_map_value(map_type, tmp)}).then([](auto rs) {
        truncation_records = {};
        return force_blocking_flush(LOCAL);
    });
}

/**
 * This method is used to remove information about truncation time for specified column family
 */
future<> remove_truncation_record(utils::UUID id) {
    sstring req = sprint("DELETE truncated_at[?] from system.%s WHERE key = '%s'", LOCAL, LOCAL);
    return qctx->qp().execute_internal(req, {id}).then([](auto rs) {
        truncation_records = {};
        return force_blocking_flush(LOCAL);
    });
}

static future<truncation_entry> get_truncation_record(utils::UUID cf_id) {
    if (!truncation_records) {
        sstring req = sprint("SELECT truncated_at FROM system.%s WHERE key = '%s'", LOCAL, LOCAL);
        return qctx->qp().execute_internal(req).then([cf_id](::shared_ptr<cql3::untyped_result_set> rs) {
            truncation_map tmp;
            if (!rs->empty() && rs->one().has("truncated_at")) {
                auto map = rs->one().get_map<utils::UUID, bytes>("truncated_at");
                for (auto& p : map) {
                    auto uuid = p.first;
                    auto buf = p.second;

                    truncation_entry e;

                    data_input in(buf);

                    while (in.avail() > sizeof(db_clock::rep)) {
                        e.first.emplace_back(db::serializer<replay_position>::read(in));
                    }
                    e.second = db_clock::time_point(db_clock::duration(in.read<db_clock::rep>()));
                    tmp[uuid] = e;
                }
            }
            truncation_records = std::move(tmp);
            return get_truncation_record(cf_id);
        });
    }
    return make_ready_future<truncation_entry>((*truncation_records)[cf_id]);
}

future<> save_truncation_record(const column_family& cf, db_clock::time_point truncated_at, db::replay_position rp) {
    // TODO: this is horribly ineffective, we're doing a full flush of all system tables for all cores
    // once, for each core (calling us). But right now, redesigning so that calling here (or, rather,
    // save_truncation_records), is done from "somewhere higher, once per machine, not shard" is tricky.
    // Mainly because drop_tables also uses truncate. And is run per-core as well. Gah.
    return get_truncated_position(cf.schema()->id()).then([&cf, truncated_at, rp](replay_positions positions) {
        auto i = std::find_if(positions.begin(), positions.end(), [rp](auto& p) {
            return p.shard_id() == rp.shard_id();
        });
        if (i == positions.end()) {
            positions.emplace_back(rp);
        } else {
            *i = rp;
        }
        return save_truncation_records(cf, truncated_at, positions);
    });
}

future<db::replay_position> get_truncated_position(utils::UUID cf_id, uint32_t shard) {
    return get_truncated_position(std::move(cf_id)).then([shard](replay_positions positions) {
       for (auto& rp : positions) {
           if (shard == rp.shard_id()) {
               return make_ready_future<db::replay_position>(rp);
           }
       }
       return make_ready_future<db::replay_position>();
    });
}

 future<replay_positions> get_truncated_position(utils::UUID cf_id) {
    return get_truncation_record(cf_id).then([](truncation_entry e) {
        return make_ready_future<replay_positions>(e.first);
    });
}

future<db_clock::time_point> get_truncated_at(utils::UUID cf_id) {
    return get_truncation_record(cf_id).then([](truncation_entry e) {
        return make_ready_future<db_clock::time_point>(e.second);
    });
}

set_type_impl::native_type prepare_tokens(std::unordered_set<dht::token>& tokens) {
    set_type_impl::native_type tset;
    for (auto& t: tokens) {
        tset.push_back(dht::global_partitioner().to_sstring(t));
    }
    return tset;
}

std::unordered_set<dht::token> decode_tokens(set_type_impl::native_type& tokens) {
    std::unordered_set<dht::token> tset;
    for (auto& t: tokens) {
        auto str = value_cast<sstring>(t);
        assert(str == dht::global_partitioner().to_sstring(dht::global_partitioner().from_sstring(str)));
        tset.insert(dht::global_partitioner().from_sstring(str));
    }
    return tset;
}

/**
 * Record tokens being used by another node
 */
future<> update_tokens(gms::inet_address ep, std::unordered_set<dht::token> tokens)
{
    if (ep == utils::fb_utilities::get_broadcast_address()) {
        return remove_endpoint(ep);
    }

    sstring req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
    auto set_type = set_type_impl::get_instance(utf8_type, true);
    return execute_cql(req, PEERS, ep.addr(), make_set_value(set_type, prepare_tokens(tokens))).discard_result().then([] {
        return force_blocking_flush(PEERS);
    });
}

future<std::unordered_set<dht::token>> update_local_tokens(
    const std::unordered_set<dht::token> add_tokens,
    const std::unordered_set<dht::token> rm_tokens) {
    return get_saved_tokens().then([add_tokens = std::move(add_tokens), rm_tokens = std::move(rm_tokens)] (auto tokens) {
        for (auto& x : rm_tokens) {
            tokens.erase(x);
        }
        for (auto& x : add_tokens) {
            tokens.insert(x);
        }
        return update_tokens(tokens).then([tokens] {
            return tokens;
        });
    });
}

future<std::unordered_map<gms::inet_address, std::unordered_set<dht::token>>> load_tokens() {
    sstring req = "SELECT peer, tokens FROM system.%s";
    return execute_cql(req, PEERS).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        auto ret = make_lw_shared<std::unordered_map<gms::inet_address, std::unordered_set<dht::token>>>();
        return do_for_each(*msg, [ret] (auto& row) {
            auto peer = gms::inet_address(row.template get_as<net::ipv4_address>("peer"));
            if (row.has("tokens")) {
                auto blob = row.get_blob("tokens");
                auto cdef = peers()->get_column_definition("tokens");
                auto deserialized = cdef->type->deserialize(blob);
                auto tokens = value_cast<set_type_impl::native_type>(deserialized);

                ret->emplace(peer, decode_tokens(tokens));
            }
            return make_ready_future<>();
        }).then([ret, msg] () mutable {
            return std::move(*ret);
        });
    });
}

future<std::unordered_map<gms::inet_address, utils::UUID>> load_host_ids() {
    sstring req = "SELECT peer, host_id FROM system.%s";
    return execute_cql(req, PEERS).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        auto ret = make_lw_shared<std::unordered_map<gms::inet_address, utils::UUID>>();
        return do_for_each(*msg, [ret] (auto& row) {
            auto peer = gms::inet_address(row.template get_as<net::ipv4_address>("peer"));
            if (row.has("host_id")) {
                ret->emplace(peer, row.template get_as<utils::UUID>("host_id"));
            }
            return make_ready_future<>();
        }).then([ret, msg] () mutable {
            return std::move(*ret);
        });
    });
}

future<> update_preferred_ip(gms::inet_address ep, gms::inet_address preferred_ip) {
    sstring req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
    return execute_cql(req, PEERS, ep.addr(), preferred_ip.addr()).discard_result().then([] {
        return force_blocking_flush(PEERS);
    });
}

future<std::unordered_map<gms::inet_address, gms::inet_address>> get_preferred_ips() {
    sstring req = "SELECT peer, preferred_ip FROM system.%s";

    return execute_cql(req, PEERS).then([] (::shared_ptr<cql3::untyped_result_set> cql_res_set) {
        std::unordered_map<gms::inet_address, gms::inet_address> res;

        for (auto& r : *cql_res_set) {
            if (r.has("preferred_ip")) {
                res.emplace(gms::inet_address(r.get_as<net::ipv4_address>("peer")),
                            gms::inet_address(r.get_as<net::ipv4_address>("preferred_ip")));
            }
        }

        return res;
    });
}

template <typename Value>
static future<> update_cached_values(gms::inet_address ep, sstring column_name, Value value) {
    return make_ready_future<>();
}

template <>
future<> update_cached_values(gms::inet_address ep, sstring column_name, sstring value) {
    return _local_cache.invoke_on_all([ep = std::move(ep),
                                       column_name = std::move(column_name),
                                       value = std::move(value)] (local_cache& lc) {
        if (column_name == "data_center") {
            lc._cached_dc_rack_info[ep].dc = value;
        } else if (column_name == "rack") {
            lc._cached_dc_rack_info[ep].rack = value;
        }
        return make_ready_future<>();
    });
}

template <typename Value>
future<> update_peer_info(gms::inet_address ep, sstring column_name, Value value) {
    if (ep == utils::fb_utilities::get_broadcast_address()) {
        return make_ready_future<>();
    }

    return update_cached_values(ep, column_name, value).then([ep, column_name, value] {
        sstring clause = sprint("(peer, %s) VALUES (?, ?)", column_name);
        sstring req = "INSERT INTO system.%s " + clause;
        return execute_cql(req, PEERS, ep.addr(), value).discard_result();
    });
}
// sets are not needed, since tokens are updated by another method
template future<> update_peer_info<sstring>(gms::inet_address ep, sstring column_name, sstring);
template future<> update_peer_info<utils::UUID>(gms::inet_address ep, sstring column_name, utils::UUID);
template future<> update_peer_info<net::ipv4_address>(gms::inet_address ep, sstring column_name, net::ipv4_address);

future<> update_hints_dropped(gms::inet_address ep, utils::UUID time_period, int value) {
    // with 30 day TTL
    sstring req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
    return execute_cql(req, PEER_EVENTS, time_period, value, ep.addr()).discard_result();
}

future<> update_schema_version(utils::UUID version) {
    sstring req = "INSERT INTO system.%s (key, schema_version) VALUES (?, ?)";
    return execute_cql(req, LOCAL, sstring(LOCAL), version).discard_result();
}

/**
 * Remove stored tokens being used by another node
 */
future<> remove_endpoint(gms::inet_address ep) {
    return _local_cache.invoke_on_all([ep] (local_cache& lc) {
        lc._cached_dc_rack_info.erase(ep);
    }).then([ep] {
        sstring req = "DELETE FROM system.%s WHERE peer = ?";
        return execute_cql(req, PEERS, ep.addr()).discard_result();
    }).then([] {
        return force_blocking_flush(PEERS);
    });
}

    /**
     * This method is used to update the System Keyspace with the new tokens for this node
    */
future<> update_tokens(std::unordered_set<dht::token> tokens) {
    if (tokens.empty()) {
        throw std::invalid_argument("remove_endpoint should be used instead");
    }

    sstring req = "INSERT INTO system.%s (key, tokens) VALUES (?, ?)";
    auto set_type = set_type_impl::get_instance(utf8_type, true);
    return execute_cql(req, LOCAL, sstring(LOCAL), make_set_value(set_type, prepare_tokens(tokens))).discard_result().then([] {
        return force_blocking_flush(LOCAL);
    });
}

future<> force_blocking_flush(sstring cfname) {
    assert(qctx);
    return qctx->_db.invoke_on_all([cfname = std::move(cfname)](database& db) {
        // if (!Boolean.getBoolean("cassandra.unsafesystem"))
        column_family& cf = db.find_column_family(NAME, cfname);
        return cf.flush();
    });
}

/**
 * One of three things will happen if you try to read the system keyspace:
 * 1. files are present and you can read them: great
 * 2. no files are there: great (new node is assumed)
 * 3. files are present but you can't read them: bad
 */
future<> check_health() {
    using namespace transport::messages;
    sstring req = "SELECT cluster_name FROM system.%s WHERE key=?";
    return execute_cql(req, LOCAL, sstring(LOCAL)).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        if (msg->empty() || !msg->one().has("cluster_name")) {
            // this is a brand new node
            sstring ins_req = "INSERT INTO system.%s (key, cluster_name) VALUES (?, ?)";
            return execute_cql(ins_req, LOCAL, sstring(LOCAL), qctx->db().get_config().cluster_name()).discard_result();
        } else {
            auto saved_cluster_name = msg->one().get_as<sstring>("cluster_name");
            auto cluster_name = qctx->db().get_config().cluster_name();

            if (cluster_name != saved_cluster_name) {
                throw exceptions::configuration_exception("Saved cluster name " + saved_cluster_name + " != configured name " + cluster_name);
            }

            return make_ready_future<>();
        }
    });
}

future<std::unordered_set<dht::token>> get_saved_tokens() {
    sstring req = "SELECT tokens FROM system.%s WHERE key = ?";
    return execute_cql(req, LOCAL, sstring(LOCAL)).then([] (auto msg) {
        if (msg->empty() || !msg->one().has("tokens")) {
            return make_ready_future<std::unordered_set<dht::token>>();
        }

        auto blob = msg->one().get_blob("tokens");
        auto cdef = local()->get_column_definition("tokens");
        auto deserialized = cdef->type->deserialize(blob);
        auto tokens = value_cast<set_type_impl::native_type>(deserialized);

        return make_ready_future<std::unordered_set<dht::token>>(decode_tokens(tokens));
    });
}

bool bootstrap_complete() {
    return get_bootstrap_state() == bootstrap_state::COMPLETED;
}

bool bootstrap_in_progress() {
    return get_bootstrap_state() == bootstrap_state::IN_PROGRESS;
}

bool was_decommissioned() {
    return get_bootstrap_state() == bootstrap_state::DECOMMISSIONED;
}

bootstrap_state get_bootstrap_state() {
    return _local_cache.local()._state;
}

future<> set_bootstrap_state(bootstrap_state state) {
    static std::unordered_map<bootstrap_state, sstring, enum_hash<bootstrap_state>> state_to_name({
        { bootstrap_state::NEEDS_BOOTSTRAP, "NEEDS_BOOTSTRAP" },
        { bootstrap_state::COMPLETED, "COMPLETED" },
        { bootstrap_state::IN_PROGRESS, "IN_PROGRESS" },
        { bootstrap_state::DECOMMISSIONED, "DECOMMISSIONED" }
    });

    sstring state_name = state_to_name.at(state);

    sstring req = "INSERT INTO system.%s (key, bootstrapped) VALUES (?, ?)";
    return execute_cql(req, LOCAL, sstring(LOCAL), state_name).discard_result().then([state] {
        return force_blocking_flush(LOCAL).then([state] {
            return _local_cache.invoke_on_all([state] (local_cache& lc) {
                lc._state = state;
            });
        });
    });
}

std::vector<schema_ptr> all_tables() {
    std::vector<schema_ptr> r;
    auto legacy_tables = db::schema_tables::all_tables();
    std::copy(legacy_tables.begin(), legacy_tables.end(), std::back_inserter(r));
    r.push_back(built_indexes());
    r.push_back(hints());
    r.push_back(batchlog());
    r.push_back(paxos());
    r.push_back(local());
    r.push_back(peers());
    r.push_back(peer_events());
    r.push_back(range_xfers());
    r.push_back(compactions_in_progress());
    r.push_back(compaction_history());
    r.push_back(sstable_activity());
    r.push_back(size_estimates());
    return r;
}

void make(database& db, bool durable, bool volatile_testing_only) {
    auto ksm = make_lw_shared<keyspace_metadata>(NAME,
            "org.apache.cassandra.locator.LocalStrategy",
            std::map<sstring, sstring>{},
            durable
            );
    auto kscfg = db.make_keyspace_config(*ksm);
    kscfg.enable_disk_reads = !volatile_testing_only;
    kscfg.enable_disk_writes = !volatile_testing_only;
    kscfg.enable_commitlog = !volatile_testing_only;
    kscfg.enable_cache = true;
    keyspace _ks{ksm, std::move(kscfg)};
    auto rs(locator::abstract_replication_strategy::create_replication_strategy(NAME, "LocalStrategy", service::get_local_storage_service().get_token_metadata(), ksm->strategy_options()));
    _ks.set_replication_strategy(std::move(rs));
    db.add_keyspace(NAME, std::move(_ks));
    auto& ks = db.find_keyspace(NAME);
    for (auto&& table : all_tables()) {
        db.add_column_family(table, ks.make_column_family_config(*table));
    }
}

future<utils::UUID> get_local_host_id() {
    using namespace transport::messages;
    sstring req = "SELECT host_id FROM system.%s WHERE key=?";
    return execute_cql(req, LOCAL, sstring(LOCAL)).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        auto new_id = [] {
            auto host_id = utils::make_random_uuid();
            return set_local_host_id(host_id);
        };
        if (msg->empty() || !msg->one().has("host_id")) {
            return new_id();
        }

        auto host_id = msg->one().get_as<utils::UUID>("host_id");
        return make_ready_future<utils::UUID>(host_id);
    });
}

future<utils::UUID> set_local_host_id(const utils::UUID& host_id) {
    sstring req = "INSERT INTO system.%s (key, host_id) VALUES (?, ?)";
    return execute_cql(req, LOCAL, sstring(LOCAL), host_id).then([] (auto msg) {
        return force_blocking_flush(LOCAL);
    }).then([host_id] {
        return host_id;
    });
}

std::unordered_map<gms::inet_address, locator::endpoint_dc_rack>
load_dc_rack_info() {
    return _local_cache.local()._cached_dc_rack_info;
}

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>>
query_mutations(distributed<service::storage_proxy>& proxy, const sstring& cf_name) {
    database& db = proxy.local().get_db().local();
    schema_ptr schema = db.find_schema(db::system_keyspace::NAME, cf_name);
    auto slice = partition_slice_builder(*schema).build();
    auto cmd = make_lw_shared<query::read_command>(schema->id(), std::move(slice), std::numeric_limits<uint32_t>::max());
    return proxy.local().query_mutations_locally(cmd, query::full_partition_range);
}

future<lw_shared_ptr<query::result_set>>
query(distributed<service::storage_proxy>& proxy, const sstring& cf_name) {
    database& db = proxy.local().get_db().local();
    schema_ptr schema = db.find_schema(db::system_keyspace::NAME, cf_name);
    auto slice = partition_slice_builder(*schema).build();
    auto cmd = make_lw_shared<query::read_command>(schema->id(), std::move(slice), std::numeric_limits<uint32_t>::max());
    return proxy.local().query(schema, cmd, {query::full_partition_range}, db::consistency_level::ONE).then([schema, cmd] (auto&& result) {
        return make_lw_shared(query::result_set::from_raw_result(schema, cmd->slice, *result));
    });
}

future<lw_shared_ptr<query::result_set>>
query(distributed<service::storage_proxy>& proxy, const sstring& cf_name, const dht::decorated_key& key, query::clustering_range row_range)
{
    auto&& db = proxy.local().get_db().local();
    auto schema = db.find_schema(db::system_keyspace::NAME, cf_name);
    auto slice = partition_slice_builder(*schema)
        .with_range(std::move(row_range))
        .build();
    auto cmd = make_lw_shared<query::read_command>(schema->id(), std::move(slice), query::max_rows);
    return proxy.local().query(schema, cmd, {query::partition_range::make_singular(key)}, db::consistency_level::ONE).then([schema, cmd] (auto&& result) {
        return make_lw_shared(query::result_set::from_raw_result(schema, cmd->slice, *result));
    });
}

static map_type_impl::native_type prepare_rows_merged(std::unordered_map<int32_t, int64_t>& rows_merged) {
    map_type_impl::native_type tmp;
    for (auto& r: rows_merged) {
        int32_t first = r.first;
        int64_t second = r.second;
        auto map_element = std::make_pair<data_value, data_value>(data_value(first), data_value(second));
        tmp.push_back(std::move(map_element));
    }
    return tmp;
}

future<> update_compaction_history(sstring ksname, sstring cfname, int64_t compacted_at, int64_t bytes_in, int64_t bytes_out,
                                   std::unordered_map<int32_t, int64_t> rows_merged)
{
    // don't write anything when the history table itself is compacted, since that would in turn cause new compactions
    if (ksname == "system" && cfname == COMPACTION_HISTORY) {
        return make_ready_future<>();
    }

    auto map_type = map_type_impl::get_instance(int32_type, long_type, true);

    sstring req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged) VALUES (?, ?, ?, ?, ?, ?, ?)";

    return execute_cql(req, COMPACTION_HISTORY, utils::UUID_gen::get_time_UUID(), ksname, cfname, compacted_at, bytes_in, bytes_out,
                       make_map_value(map_type, prepare_rows_merged(rows_merged))).discard_result();
}

future<std::vector<compaction_history_entry>> get_compaction_history()
{
    sstring req = "SELECT * from system.%s";
    return execute_cql(req, COMPACTION_HISTORY).then([] (::shared_ptr<cql3::untyped_result_set> msg) {
        std::vector<compaction_history_entry> history;

        for (auto& row : *msg) {
            compaction_history_entry entry;
            entry.id = row.get_as<utils::UUID>("id");
            entry.ks = row.get_as<sstring>("keyspace_name");
            entry.cf = row.get_as<sstring>("columnfamily_name");
            entry.compacted_at = row.get_as<int64_t>("compacted_at");
            entry.bytes_in = row.get_as<int64_t>("bytes_in");
            entry.bytes_out = row.get_as<int64_t>("bytes_out");
            if (row.has("rows_merged")) {
                entry.rows_merged = row.get_map<int32_t, int64_t>("rows_merged");
            }
            history.push_back(std::move(entry));
        }
        return std::move(history);
    });
}

} // namespace system_keyspace
} // namespace db
