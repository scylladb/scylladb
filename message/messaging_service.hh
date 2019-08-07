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
#include "core/reactor.hh"
#include "core/distributed.hh"
#include "core/sstring.hh"
#include "gms/inet_address.hh"
#include "rpc/rpc_types.hh"
#include <unordered_map>
#include "query-request.hh"
#include "mutation_query.hh"
#include "range.hh"
#include "repair/repair.hh"
#include "tracing/tracing.hh"
#include "digest_algorithm.hh"
#include "streaming/stream_reason.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"

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
    LAST = 26,
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

    void register_stream_mutation_done(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, dht::token_range_vector ranges, UUID cf_id, unsigned dst_cpu_id)>&& func);
    future<> send_stream_mutation_done(msg_addr id, UUID plan_id, dht::token_range_vector ranges, UUID cf_id, unsigned dst_cpu_id);

    void register_complete_message(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id, rpc::optional<bool> failed)>&& func);
    future<> send_complete_message(msg_addr id, UUID plan_id, unsigned dst_cpu_id, bool failed = false);

    // Wrapper for REPAIR_CHECKSUM_RANGE verb
    void register_repair_checksum_range(std::function<future<partition_checksum> (sstring keyspace, sstring cf, dht::token_range range, rpc::optional<repair_checksum> hash_version)>&& func);
    void unregister_repair_checksum_range();
    future<partition_checksum> send_repair_checksum_range(msg_addr id, sstring keyspace, sstring cf, dht::token_range range, repair_checksum hash_version);

    // Wrapper for GOSSIP_ECHO verb
    void register_gossip_echo(std::function<future<> ()>&& func);
    void unregister_gossip_echo();
    future<> send_gossip_echo(msg_addr id);

    // Wrapper for GOSSIP_SHUTDOWN
    void register_gossip_shutdown(std::function<rpc::no_wait_type (inet_address from)>&& func);
    void unregister_gossip_shutdown();
    future<> send_gossip_shutdown(msg_addr id, inet_address from);

    // Wrapper for GOSSIP_DIGEST_SYN
    void register_gossip_digest_syn(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, gms::gossip_digest_syn)>&& func);
    void unregister_gossip_digest_syn();
    future<> send_gossip_digest_syn(msg_addr id, gms::gossip_digest_syn msg);

    // Wrapper for GOSSIP_DIGEST_ACK
    void register_gossip_digest_ack(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, gms::gossip_digest_ack)>&& func);
    void unregister_gossip_digest_ack();
    future<> send_gossip_digest_ack(msg_addr id, gms::gossip_digest_ack msg);

    // Wrapper for GOSSIP_DIGEST_ACK2
    void register_gossip_digest_ack2(std::function<rpc::no_wait_type (gms::gossip_digest_ack2)>&& func);
    void unregister_gossip_digest_ack2();
    future<> send_gossip_digest_ack2(msg_addr id, gms::gossip_digest_ack2 msg);

    // Wrapper for DEFINITIONS_UPDATE
    void register_definitions_update(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, std::vector<frozen_mutation> fm)>&& func);
    void unregister_definitions_update();
    future<> send_definitions_update(msg_addr id, std::vector<frozen_mutation> fm);

    // Wrapper for MIGRATION_REQUEST
    void register_migration_request(std::function<future<std::vector<frozen_mutation>> (const rpc::client_info&)>&& func);
    void unregister_migration_request();
    future<std::vector<frozen_mutation>> send_migration_request(msg_addr id);

    // FIXME: response_id_type is an alias in service::storage_proxy::response_id_type
    using response_id_type = uint64_t;
    // Wrapper for MUTATION
    void register_mutation(std::function<future<rpc::no_wait_type> (const rpc::client_info&, rpc::opt_time_point, frozen_mutation fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id, rpc::optional<std::experimental::optional<tracing::trace_info>> trace_info)>&& func);
    void unregister_mutation();
    future<> send_mutation(msg_addr id, clock_type::time_point timeout, const frozen_mutation& fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id, std::experimental::optional<tracing::trace_info> trace_info = std::experimental::nullopt);

    // Wrapper for COUNTER_MUTATION
    void register_counter_mutation(std::function<future<> (const rpc::client_info&, rpc::opt_time_point, std::vector<frozen_mutation> fms, db::consistency_level cl, stdx::optional<tracing::trace_info> trace_info)>&& func);
    void unregister_counter_mutation();
    future<> send_counter_mutation(msg_addr id, clock_type::time_point timeout, std::vector<frozen_mutation> fms, db::consistency_level cl, stdx::optional<tracing::trace_info> trace_info = std::experimental::nullopt);

    // Wrapper for MUTATION_DONE
    void register_mutation_done(std::function<future<rpc::no_wait_type> (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id, rpc::optional<db::view::update_backlog> backlog)>&& func);
    void unregister_mutation_done();
    future<> send_mutation_done(msg_addr id, unsigned shard, response_id_type response_id, db::view::update_backlog backlog);

    // Wrapper for MUTATION_FAILED
    void register_mutation_failed(std::function<future<rpc::no_wait_type> (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id, size_t num_failed, rpc::optional<db::view::update_backlog> backlog)>&& func);
    void unregister_mutation_failed();
    future<> send_mutation_failed(msg_addr id, unsigned shard, response_id_type response_id, size_t num_failed, db::view::update_backlog backlog);

    // Wrapper for READ_DATA
    // Note: WTH is future<foreign_ptr<lw_shared_ptr<query::result>>
    void register_read_data(std::function<future<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature> (const rpc::client_info&, rpc::opt_time_point timeout, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> digest)>&& func);
    void unregister_read_data();
    future<query::result, rpc::optional<cache_temperature>> send_read_data(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr, query::digest_algorithm da);

    // Wrapper for GET_SCHEMA_VERSION
    void register_get_schema_version(std::function<future<frozen_schema>(unsigned, table_schema_version)>&& func);
    void unregister_get_schema_version();
    future<frozen_schema> send_get_schema_version(msg_addr, table_schema_version);

    // Wrapper for SCHEMA_CHECK
    void register_schema_check(std::function<future<utils::UUID>()>&& func);
    void unregister_schema_check();
    future<utils::UUID> send_schema_check(msg_addr);

    // Wrapper for READ_MUTATION_DATA
    void register_read_mutation_data(std::function<future<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature> (const rpc::client_info&, rpc::opt_time_point timeout, query::read_command cmd, ::compat::wrapping_partition_range pr)>&& func);
    void unregister_read_mutation_data();
    future<reconcilable_result, rpc::optional<cache_temperature>> send_read_mutation_data(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr);

    // Wrapper for READ_DIGEST
    void register_read_digest(std::function<future<query::result_digest, api::timestamp_type, cache_temperature> (const rpc::client_info&, rpc::opt_time_point timeout, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> digest)>&& func);
    void unregister_read_digest();
    future<query::result_digest, rpc::optional<api::timestamp_type>, rpc::optional<cache_temperature>> send_read_digest(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr, query::digest_algorithm da);

    // Wrapper for TRUNCATE
    void register_truncate(std::function<future<>(sstring, sstring)>&& func);
    void unregister_truncate();
    future<> send_truncate(msg_addr, std::chrono::milliseconds, sstring, sstring);

    // Wrapper for REPLICATION_FINISHED verb
    void register_replication_finished(std::function<future<> (inet_address from)>&& func);
    void unregister_replication_finished();
    future<> send_replication_finished(msg_addr id, inet_address from);
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
