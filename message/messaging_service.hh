/*
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include "messaging_service_fwd.hh"
#include "msg_addr.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include <seastar/rpc/rpc_types.hh>
#include <unordered_map>
#include "query-request.hh"
#include "mutation_query.hh"
#include "range.hh"
#include "repair/repair.hh"
#include "tracing/tracing.hh"
#include "digest_algorithm.hh"
#include "streaming/stream_reason.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "cache_temperature.hh"
#include "service/paxos/prepare_response.hh"

#include <list>
#include <vector>
#include <optional>
#include <seastar/net/tls.hh>

// forward declarations
namespace streaming {
    class prepare_message;
}

namespace gms {
    class gossip_digest_syn;
    class gossip_digest_ack;
    class gossip_digest_ack2;
}

namespace utils {
    class UUID;
}

namespace db {
class seed_provider_type;
}

namespace db::view {
class update_backlog;
}

class frozen_mutation;
class frozen_schema;
class partition_checksum;

namespace dht {
    class token;
}

namespace query {
    using partition_range = dht::partition_range;
    class read_command;
    class result;
}

namespace compat {

using wrapping_partition_range = wrapping_range<dht::ring_position>;

}

namespace netw {

/* All verb handler identifiers */
enum class messaging_verb : int32_t {
    CLIENT_ID = 0,
    MUTATION = 1,
    MUTATION_DONE = 2,
    READ_DATA = 3,
    READ_MUTATION_DATA = 4,
    READ_DIGEST = 5,
    // Used by gossip
    GOSSIP_DIGEST_SYN = 6,
    GOSSIP_DIGEST_ACK = 7,
    GOSSIP_DIGEST_ACK2 = 8,
    GOSSIP_ECHO = 9,
    GOSSIP_SHUTDOWN = 10,
    // end of gossip verb
    DEFINITIONS_UPDATE = 11,
    TRUNCATE = 12,
    REPLICATION_FINISHED = 13,
    MIGRATION_REQUEST = 14,
    // Used by streaming
    PREPARE_MESSAGE = 15,
    PREPARE_DONE_MESSAGE = 16,
    STREAM_MUTATION = 17,
    STREAM_MUTATION_DONE = 18,
    COMPLETE_MESSAGE = 19,
    // end of streaming verbs
    REPAIR_CHECKSUM_RANGE = 20,
    GET_SCHEMA_VERSION = 21,
    SCHEMA_CHECK = 22,
    COUNTER_MUTATION = 23,
    MUTATION_FAILED = 24,
    STREAM_MUTATION_FRAGMENTS = 25,
    REPAIR_ROW_LEVEL_START = 26,
    REPAIR_ROW_LEVEL_STOP = 27,
    REPAIR_GET_FULL_ROW_HASHES = 28,
    REPAIR_GET_COMBINED_ROW_HASH = 29,
    REPAIR_GET_SYNC_BOUNDARY = 30,
    REPAIR_GET_ROW_DIFF = 31,
    REPAIR_PUT_ROW_DIFF = 32,
    REPAIR_GET_ESTIMATED_PARTITIONS= 33,
    REPAIR_SET_ESTIMATED_PARTITIONS= 34,
    REPAIR_GET_DIFF_ALGORITHMS = 35,
    REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM = 36,
    REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM = 37,
    REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM = 38,
    PAXOS_PREPARE = 39,
    PAXOS_ACCEPT = 40,
    PAXOS_LEARN = 41,
    HINT_MUTATION = 42,
    PAXOS_PRUNE = 43,
    LAST = 44,
};

} // namespace netw

namespace std {
template <>
class hash<netw::messaging_verb> {
public:
    size_t operator()(const netw::messaging_verb& x) const {
        return hash<int32_t>()(int32_t(x));
    }
};
} // namespace std

namespace netw {

struct serializer {};

struct schema_pull_options {
    bool remote_supports_canonical_mutation_retval = true;
};

class messaging_service : public seastar::async_sharded_service<messaging_service> {
public:
    struct rpc_protocol_wrapper;
    struct rpc_protocol_client_wrapper;
    struct rpc_protocol_server_wrapper;
    struct shard_info;

    using msg_addr = netw::msg_addr;
    using inet_address = gms::inet_address;
    using UUID = utils::UUID;
    using clients_map = std::unordered_map<msg_addr, shard_info, msg_addr::hash>;

    // This should change only if serialization format changes
    static constexpr int32_t current_version = 0;

    struct shard_info {
        shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client);
        shared_ptr<rpc_protocol_client_wrapper> rpc_client;
        rpc::stats get_stats() const;
    };

    void foreach_client(std::function<void(const msg_addr& id, const shard_info& info)> f) const;

    void increment_dropped_messages(messaging_verb verb);

    uint64_t get_dropped_messages(messaging_verb verb) const;

    const uint64_t* get_dropped_messages() const;

    int32_t get_raw_version(const gms::inet_address& endpoint) const;

    bool knows_version(const gms::inet_address& endpoint) const;

    enum class encrypt_what {
        none,
        rack,
        dc,
        all,
    };

    enum class compress_what {
        none,
        dc,
        all,
    };

    enum class tcp_nodelay_what {
        local,
        all,
    };

    struct memory_config {
        size_t rpc_memory_limit = 1'000'000;
    };

    struct scheduling_config {
        scheduling_group statement;
        scheduling_group streaming;
        scheduling_group gossip;
    };

private:
    gms::inet_address _listen_address;
    uint16_t _port;
    uint16_t _ssl_port;
    encrypt_what _encrypt_what;
    compress_what _compress_what;
    tcp_nodelay_what _tcp_nodelay_what;
    bool _should_listen_to_broadcast_address;
    // map: Node broadcast address -> Node internal IP for communication within the same data center
    std::unordered_map<gms::inet_address, gms::inet_address> _preferred_ip_cache;
    std::unique_ptr<rpc_protocol_wrapper> _rpc;
    std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;
    std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server_tls;
    std::array<clients_map, 4> _clients;
    uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::LAST)] = {};
    bool _stopping = false;
    std::list<std::function<void(gms::inet_address ep)>> _connection_drop_notifiers;
    memory_config _mcfg;
    scheduling_config _scheduling_config;
public:
    using clock_type = lowres_clock;
public:
    messaging_service(gms::inet_address ip = gms::inet_address("0.0.0.0"),
            uint16_t port = 7000, bool listen_now = true);
    messaging_service(gms::inet_address ip, uint16_t port, encrypt_what, compress_what, tcp_nodelay_what,
            uint16_t ssl_port, std::shared_ptr<seastar::tls::credentials_builder>,
            memory_config mcfg, scheduling_config scfg, bool sltba = false, bool listen_now = true);
    ~messaging_service();
public:
    void start_listen();
    uint16_t port();
    gms::inet_address listen_address();
    future<> stop_tls_server();
    future<> stop_nontls_server();
    future<> stop_client();
    future<> stop();
    static rpc::no_wait_type no_wait();
    bool is_stopping() { return _stopping; }
public:
    gms::inet_address get_preferred_ip(gms::inet_address ep);
    future<> init_local_preferred_ip_cache();
    void cache_preferred_ip(gms::inet_address ep, gms::inet_address ip);

    future<> unregister_handler(messaging_verb verb);

    // Wrapper for PREPARE_MESSAGE verb
    void register_prepare_message(std::function<future<streaming::prepare_message> (const rpc::client_info& cinfo,
            streaming::prepare_message msg, UUID plan_id, sstring description, rpc::optional<streaming::stream_reason> reason)>&& func);
    future<streaming::prepare_message> send_prepare_message(msg_addr id, streaming::prepare_message msg, UUID plan_id,
            sstring description, streaming::stream_reason);

    // Wrapper for PREPARE_DONE_MESSAGE verb
    void register_prepare_done_message(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id)>&& func);
    future<> send_prepare_done_message(msg_addr id, UUID plan_id, unsigned dst_cpu_id);

    // Wrapper for STREAM_MUTATION verb
    void register_stream_mutation(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id, rpc::optional<bool>, rpc::optional<streaming::stream_reason>)>&& func);
    future<> send_stream_mutation(msg_addr id, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id, bool fragmented, streaming::stream_reason reason);

    // Wrapper for STREAM_MUTATION_FRAGMENTS
    // The receiver of STREAM_MUTATION_FRAGMENTS sends status code to the sender to notify any error on the receiver side. The status code is of type int32_t. 0 means successful, -1 means error, other status code value are reserved for future use.
    void register_stream_mutation_fragments(std::function<future<rpc::sink<int32_t>> (const rpc::client_info& cinfo, UUID plan_id, UUID schema_id, UUID cf_id, uint64_t estimated_partitions, rpc::optional<streaming::stream_reason> reason_opt, rpc::source<frozen_mutation_fragment, rpc::optional<streaming::stream_mutation_fragments_cmd>> source)>&& func);
    rpc::sink<int32_t> make_sink_for_stream_mutation_fragments(rpc::source<frozen_mutation_fragment, rpc::optional<streaming::stream_mutation_fragments_cmd>>& source);
    future<rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, rpc::source<int32_t>> make_sink_and_source_for_stream_mutation_fragments(utils::UUID schema_id, utils::UUID plan_id, utils::UUID cf_id, uint64_t estimated_partitions, streaming::stream_reason reason, msg_addr id);

    // Wrapper for REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM
    future<rpc::sink<repair_hash_with_cmd>, rpc::source<repair_row_on_wire_with_cmd>> make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(uint32_t repair_meta_id, msg_addr id);
    rpc::sink<repair_row_on_wire_with_cmd> make_sink_for_repair_get_row_diff_with_rpc_stream(rpc::source<repair_hash_with_cmd>& source);
    void register_repair_get_row_diff_with_rpc_stream(std::function<future<rpc::sink<repair_row_on_wire_with_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_hash_with_cmd> source)>&& func);

    // Wrapper for REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM
    future<rpc::sink<repair_row_on_wire_with_cmd>, rpc::source<repair_stream_cmd>> make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(uint32_t repair_meta_id, msg_addr id);
    rpc::sink<repair_stream_cmd> make_sink_for_repair_put_row_diff_with_rpc_stream(rpc::source<repair_row_on_wire_with_cmd>& source);
    void register_repair_put_row_diff_with_rpc_stream(std::function<future<rpc::sink<repair_stream_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_row_on_wire_with_cmd> source)>&& func);

    // Wrapper for REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM
    future<rpc::sink<repair_stream_cmd>, rpc::source<repair_hash_with_cmd>> make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(uint32_t repair_meta_id, msg_addr id);
    rpc::sink<repair_hash_with_cmd> make_sink_for_repair_get_full_row_hashes_with_rpc_stream(rpc::source<repair_stream_cmd>& source);
    void register_repair_get_full_row_hashes_with_rpc_stream(std::function<future<rpc::sink<repair_hash_with_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_stream_cmd> source)>&& func);

    void register_stream_mutation_done(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, dht::token_range_vector ranges, UUID cf_id, unsigned dst_cpu_id)>&& func);
    future<> send_stream_mutation_done(msg_addr id, UUID plan_id, dht::token_range_vector ranges, UUID cf_id, unsigned dst_cpu_id);

    void register_complete_message(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id, rpc::optional<bool> failed)>&& func);
    future<> send_complete_message(msg_addr id, UUID plan_id, unsigned dst_cpu_id, bool failed = false);

    // Wrapper for REPAIR_CHECKSUM_RANGE verb
    void register_repair_checksum_range(std::function<future<partition_checksum> (sstring keyspace, sstring cf, dht::token_range range, rpc::optional<repair_checksum> hash_version)>&& func);
    future<> unregister_repair_checksum_range();
    future<partition_checksum> send_repair_checksum_range(msg_addr id, sstring keyspace, sstring cf, dht::token_range range, repair_checksum hash_version);

    // Wrapper for REPAIR_GET_FULL_ROW_HASHES
    void register_repair_get_full_row_hashes(std::function<future<std::unordered_set<repair_hash>> (const rpc::client_info& cinfo, uint32_t repair_meta_id)>&& func);
    future<> unregister_repair_get_full_row_hashes();
    future<std::unordered_set<repair_hash>> send_repair_get_full_row_hashes(msg_addr id, uint32_t repair_meta_id);

    // Wrapper for REPAIR_GET_COMBINED_ROW_HASH
    void register_repair_get_combined_row_hash(std::function<future<get_combined_row_hash_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary)>&& func);
    future<> unregister_repair_get_combined_row_hash();
    future<get_combined_row_hash_response> send_repair_get_combined_row_hash(msg_addr id, uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary);

    // Wrapper for REPAIR_GET_SYNC_BOUNDARY
    void register_repair_get_sync_boundary(std::function<future<get_sync_boundary_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary)>&& func);
    future<> unregister_repair_get_sync_boundary();
    future<get_sync_boundary_response> send_repair_get_sync_boundary(msg_addr id, uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary);

    // Wrapper for REPAIR_GET_ROW_DIFF
    void register_repair_get_row_diff(std::function<future<repair_rows_on_wire> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::unordered_set<repair_hash> set_diff, bool needs_all_rows)>&& func);
    future<> unregister_repair_get_row_diff();
    future<repair_rows_on_wire> send_repair_get_row_diff(msg_addr id, uint32_t repair_meta_id, std::unordered_set<repair_hash> set_diff, bool needs_all_rows);

    // Wrapper for REPAIR_PUT_ROW_DIFF
    void register_repair_put_row_diff(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, repair_rows_on_wire row_diff)>&& func);
    future<> unregister_repair_put_row_diff();
    future<> send_repair_put_row_diff(msg_addr id, uint32_t repair_meta_id, repair_rows_on_wire row_diff);

    // Wrapper for REPAIR_ROW_LEVEL_START
    void register_repair_row_level_start(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, rpc::optional<streaming::stream_reason> reason)>&& func);
    future<> unregister_repair_row_level_start();
    future<> send_repair_row_level_start(msg_addr id, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, streaming::stream_reason reason);

    // Wrapper for REPAIR_ROW_LEVEL_STOP
    void register_repair_row_level_stop(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range)>&& func);
    future<> unregister_repair_row_level_stop();
    future<> send_repair_row_level_stop(msg_addr id, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range);

    // Wrapper for REPAIR_GET_ESTIMATED_PARTITIONS
    void register_repair_get_estimated_partitions(std::function<future<uint64_t> (const rpc::client_info& cinfo, uint32_t repair_meta_id)>&& func);
    future<> unregister_repair_get_estimated_partitions();
    future<uint64_t> send_repair_get_estimated_partitions(msg_addr id, uint32_t repair_meta_id);

    // Wrapper for REPAIR_SET_ESTIMATED_PARTITIONS
    void register_repair_set_estimated_partitions(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, uint64_t estimated_partitions)>&& func);
    future<> unregister_repair_set_estimated_partitions();
    future<> send_repair_set_estimated_partitions(msg_addr id, uint32_t repair_meta_id, uint64_t estimated_partitions);

    // Wrapper for REPAIR_GET_DIFF_ALGORITHMS
    void register_repair_get_diff_algorithms(std::function<future<std::vector<row_level_diff_detect_algorithm>> (const rpc::client_info& cinfo)>&& func);
    future<> unregister_repair_get_diff_algorithms();
    future<std::vector<row_level_diff_detect_algorithm>> send_repair_get_diff_algorithms(msg_addr id);

    // Wrapper for GOSSIP_ECHO verb
    void register_gossip_echo(std::function<future<> ()>&& func);
    future<> unregister_gossip_echo();
    future<> send_gossip_echo(msg_addr id);

    // Wrapper for GOSSIP_SHUTDOWN
    void register_gossip_shutdown(std::function<rpc::no_wait_type (inet_address from)>&& func);
    future<> unregister_gossip_shutdown();
    future<> send_gossip_shutdown(msg_addr id, inet_address from);

    // Wrapper for GOSSIP_DIGEST_SYN
    void register_gossip_digest_syn(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, gms::gossip_digest_syn)>&& func);
    future<> unregister_gossip_digest_syn();
    future<> send_gossip_digest_syn(msg_addr id, gms::gossip_digest_syn msg);

    // Wrapper for GOSSIP_DIGEST_ACK
    void register_gossip_digest_ack(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, gms::gossip_digest_ack)>&& func);
    future<> unregister_gossip_digest_ack();
    future<> send_gossip_digest_ack(msg_addr id, gms::gossip_digest_ack msg);

    // Wrapper for GOSSIP_DIGEST_ACK2
    void register_gossip_digest_ack2(std::function<rpc::no_wait_type (gms::gossip_digest_ack2)>&& func);
    future<> unregister_gossip_digest_ack2();
    future<> send_gossip_digest_ack2(msg_addr id, gms::gossip_digest_ack2 msg);

    // Wrapper for DEFINITIONS_UPDATE
    void register_definitions_update(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, std::vector<frozen_mutation> fm,
                rpc::optional<std::vector<canonical_mutation>> cm)>&& func);
    future<> unregister_definitions_update();
    future<> send_definitions_update(msg_addr id, std::vector<frozen_mutation> fm, std::vector<canonical_mutation> cm);

    // Wrapper for MIGRATION_REQUEST
    void register_migration_request(std::function<future<rpc::tuple<std::vector<frozen_mutation>, std::vector<canonical_mutation>>> (
                const rpc::client_info&, rpc::optional<schema_pull_options>)>&& func);
    future<> unregister_migration_request();
    future<rpc::tuple<std::vector<frozen_mutation>, rpc::optional<std::vector<canonical_mutation>>>> send_migration_request(msg_addr id,
            schema_pull_options options);

    // FIXME: response_id_type is an alias in service::storage_proxy::response_id_type
    using response_id_type = uint64_t;
    // Wrapper for MUTATION
    void register_mutation(std::function<future<rpc::no_wait_type> (const rpc::client_info&, rpc::opt_time_point, frozen_mutation fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id, rpc::optional<std::optional<tracing::trace_info>> trace_info)>&& func);
    future<> unregister_mutation();
    future<> send_mutation(msg_addr id, clock_type::time_point timeout, const frozen_mutation& fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id, std::optional<tracing::trace_info> trace_info = std::nullopt);

    // Wrapper for COUNTER_MUTATION
    void register_counter_mutation(std::function<future<> (const rpc::client_info&, rpc::opt_time_point, std::vector<frozen_mutation> fms, db::consistency_level cl, std::optional<tracing::trace_info> trace_info)>&& func);
    future<> unregister_counter_mutation();
    future<> send_counter_mutation(msg_addr id, clock_type::time_point timeout, std::vector<frozen_mutation> fms, db::consistency_level cl, std::optional<tracing::trace_info> trace_info = std::nullopt);

    // Wrapper for MUTATION_DONE
    void register_mutation_done(std::function<future<rpc::no_wait_type> (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id, rpc::optional<db::view::update_backlog> backlog)>&& func);
    future<> unregister_mutation_done();
    future<> send_mutation_done(msg_addr id, unsigned shard, response_id_type response_id, db::view::update_backlog backlog);

    // Wrapper for MUTATION_FAILED
    void register_mutation_failed(std::function<future<rpc::no_wait_type> (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id, size_t num_failed, rpc::optional<db::view::update_backlog> backlog)>&& func);
    future<> unregister_mutation_failed();
    future<> send_mutation_failed(msg_addr id, unsigned shard, response_id_type response_id, size_t num_failed, db::view::update_backlog backlog);

    // Wrapper for READ_DATA
    // Note: WTH is future<foreign_ptr<lw_shared_ptr<query::result>>
    void register_read_data(std::function<future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> (const rpc::client_info&, rpc::opt_time_point timeout, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> digest)>&& func);
    future<> unregister_read_data();
    future<rpc::tuple<query::result, rpc::optional<cache_temperature>>> send_read_data(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr, query::digest_algorithm da);

    // Wrapper for GET_SCHEMA_VERSION
    void register_get_schema_version(std::function<future<frozen_schema>(unsigned, table_schema_version)>&& func);
    future<> unregister_get_schema_version();
    future<frozen_schema> send_get_schema_version(msg_addr, table_schema_version);

    // Wrapper for SCHEMA_CHECK
    void register_schema_check(std::function<future<utils::UUID>()>&& func);
    future<> unregister_schema_check();
    future<utils::UUID> send_schema_check(msg_addr);

    // Wrapper for READ_MUTATION_DATA
    void register_read_mutation_data(std::function<future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> (const rpc::client_info&, rpc::opt_time_point timeout, query::read_command cmd, ::compat::wrapping_partition_range pr)>&& func);
    future<> unregister_read_mutation_data();
    future<rpc::tuple<reconcilable_result, rpc::optional<cache_temperature>>> send_read_mutation_data(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr);

    // Wrapper for READ_DIGEST
    void register_read_digest(std::function<future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature>> (const rpc::client_info&, rpc::opt_time_point timeout, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> digest)>&& func);
    future<> unregister_read_digest();
    future<rpc::tuple<query::result_digest, rpc::optional<api::timestamp_type>, rpc::optional<cache_temperature>>> send_read_digest(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr, query::digest_algorithm da);

    // Wrapper for TRUNCATE
    void register_truncate(std::function<future<>(sstring, sstring)>&& func);
    future<> unregister_truncate();
    future<> send_truncate(msg_addr, std::chrono::milliseconds, sstring, sstring);

    // Wrapper for REPLICATION_FINISHED verb
    void register_replication_finished(std::function<future<> (inet_address from)>&& func);
    future<> unregister_replication_finished();
    future<> send_replication_finished(msg_addr id, inet_address from);

    // Wrappers for PAXOS verbs
    void register_paxos_prepare(std::function<future<foreign_ptr<std::unique_ptr<service::paxos::prepare_response>>>(
                const rpc::client_info&, rpc::opt_time_point, query::read_command cmd, partition_key key, utils::UUID ballot,
                bool only_digest, query::digest_algorithm da, std::optional<tracing::trace_info>)>&& func);

    future<> unregister_paxos_prepare();

    future<service::paxos::prepare_response> send_paxos_prepare(
            gms::inet_address peer, clock_type::time_point timeout, const query::read_command& cmd,
            const partition_key& key, utils::UUID ballot, bool only_digest, query::digest_algorithm da,
            std::optional<tracing::trace_info> trace_info);

    void register_paxos_accept(std::function<future<bool>(const rpc::client_info&, rpc::opt_time_point,
            service::paxos::proposal proposal, std::optional<tracing::trace_info>)>&& func);

    future<> unregister_paxos_accept();

    future<bool> send_paxos_accept(gms::inet_address peer, clock_type::time_point timeout,
            const service::paxos::proposal& proposal, std::optional<tracing::trace_info> trace_info);

    void register_paxos_learn(std::function<future<rpc::no_wait_type> (const rpc::client_info&,
                rpc::opt_time_point, service::paxos::proposal decision, std::vector<inet_address> forward, inet_address reply_to,
                unsigned shard, response_id_type response_id, std::optional<tracing::trace_info> trace_info)>&& func);

    future<> unregister_paxos_learn();

    future<> send_paxos_learn(msg_addr id, clock_type::time_point timeout, const service::paxos::proposal& decision,
            std::vector<inet_address> forward, inet_address reply_to, unsigned shard, response_id_type response_id,
            std::optional<tracing::trace_info> trace_info = std::nullopt);

    void register_paxos_prune(std::function<future<rpc::no_wait_type>(const rpc::client_info&, rpc::opt_time_point, UUID schema_id, partition_key key,
            utils::UUID ballot, std::optional<tracing::trace_info>)>&& func);

    future<> unregister_paxos_prune();

    future<> send_paxos_prune(gms::inet_address peer, clock_type::time_point timeout, UUID schema_id, const partition_key& key,
            utils::UUID ballot, std::optional<tracing::trace_info> trace_info);

    void register_hint_mutation(std::function<future<rpc::no_wait_type> (const rpc::client_info&, rpc::opt_time_point, frozen_mutation fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id, rpc::optional<std::optional<tracing::trace_info>> trace_info)>&& func);
    future<> unregister_hint_mutation();
    future<> send_hint_mutation(msg_addr id, clock_type::time_point timeout, const frozen_mutation& fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id, std::optional<tracing::trace_info> trace_info = std::nullopt);

    void foreach_server_connection_stats(std::function<void(const rpc::client_info&, const rpc::stats&)>&& f) const;
private:
    bool remove_rpc_client_one(clients_map& clients, msg_addr id, bool dead_only);
public:
    // Return rpc::protocol::client for a shard which is a ip + cpuid pair.
    shared_ptr<rpc_protocol_client_wrapper> get_rpc_client(messaging_verb verb, msg_addr id);
    void remove_error_rpc_client(messaging_verb verb, msg_addr id);
    void remove_rpc_client(msg_addr id);
    using drop_notifier_handler = decltype(_connection_drop_notifiers)::iterator;
    drop_notifier_handler register_connection_drop_notifier(std::function<void(gms::inet_address ep)> cb);
    void unregister_connection_drop_notifier(drop_notifier_handler h);
    std::unique_ptr<rpc_protocol_wrapper>& rpc();
    static msg_addr get_source(const rpc::client_info& client);
    scheduling_group scheduling_group_for_verb(messaging_verb verb) const;
};

extern distributed<messaging_service> _the_messaging_service;

inline distributed<messaging_service>& get_messaging_service() {
    return _the_messaging_service;
}

inline messaging_service& get_local_messaging_service() {
    return _the_messaging_service.local();
}

} // namespace netw
