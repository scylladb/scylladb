/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "db/config.hh"
#include "messaging_service_fwd.hh"
#include "msg_addr.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>
#include "gms/inet_address.hh"
#include <seastar/rpc/rpc_types.hh>
#include <unordered_map>
#include "gc_clock.hh"
#include "interval.hh"
#include "schema/schema_fwd.hh"
#include "streaming/stream_fwd.hh"
#include "locator/host_id.hh"
#include "service/session.hh"
#include "service/maintenance_mode.hh"
#include "tasks/types.hh"

#include <list>
#include <vector>
#include <optional>
#include <array>
#include <absl/container/btree_set.h>
#include <seastar/net/tls.hh>

// forward declarations
namespace streaming {
    class prepare_message;
    enum class stream_mutation_fragments_cmd : uint8_t;
}

namespace gms {
    class gossip_digest_syn;
    class gossip_digest_ack;
    class gossip_digest_ack2;
    class gossip_get_endpoint_states_request;
    class gossip_get_endpoint_states_response;
}

namespace db {
class seed_provider_type;
class config;
}

namespace db::view {
class update_backlog;
}

namespace locator {
class shared_token_metadata;
}

class frozen_mutation;
class frozen_schema;
class canonical_mutation;

namespace dht {
    class token;
    class ring_position;
    using partition_range = interval<ring_position>;
    using token_range = interval<token>;
    using token_range_vector = std::vector<token_range>;
}

namespace query {
    using partition_range = dht::partition_range;
    class read_command;
    class result;
}

namespace compat {

using wrapping_partition_range = wrapping_interval<dht::ring_position>;

}

class repair_hash_with_cmd;
class repair_row_on_wire_with_cmd;
enum class repair_stream_cmd : uint8_t;
class repair_stream_boundary;
class frozen_mutation_fragment;
class repair_hash;
using get_combined_row_hash_response = repair_hash;
using repair_hash_set = absl::btree_set<repair_hash>;
class repair_sync_boundary;
class get_sync_boundary_response;
class partition_key_and_mutation_fragments;
using repair_rows_on_wire = std::list<partition_key_and_mutation_fragments>;
class repair_row_level_start_response;
class node_ops_cmd_response;
class node_ops_cmd_request;
enum class row_level_diff_detect_algorithm : uint8_t;

namespace streaming {

enum class stream_reason : uint8_t;

}

namespace service {

class group0_peer_exchange;

}

namespace tasks {
using get_children_request = task_id;
using get_children_response = std::vector<task_id>;
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
    UNUSED__REPLICATION_FINISHED = 13,
    MIGRATION_REQUEST = 14,
    // Used by streaming
    PREPARE_MESSAGE = 15,
    PREPARE_DONE_MESSAGE = 16,
    UNUSED__STREAM_MUTATION = 17,
    STREAM_MUTATION_DONE = 18,
    COMPLETE_MESSAGE = 19,
    // end of streaming verbs
    UNUSED__REPAIR_CHECKSUM_RANGE = 20,
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
    GOSSIP_GET_ENDPOINT_STATES = 44,
    NODE_OPS_CMD = 45,
    RAFT_SEND_SNAPSHOT = 46,
    RAFT_APPEND_ENTRIES = 47,
    RAFT_APPEND_ENTRIES_REPLY = 48,
    RAFT_VOTE_REQUEST = 49,
    RAFT_VOTE_REPLY = 50,
    RAFT_TIMEOUT_NOW = 51,
    RAFT_READ_QUORUM = 52,
    RAFT_READ_QUORUM_REPLY = 53,
    RAFT_EXECUTE_READ_BARRIER_ON_LEADER = 54,
    RAFT_ADD_ENTRY = 55,
    RAFT_MODIFY_CONFIG = 56,
    GROUP0_PEER_EXCHANGE = 57,
    GROUP0_MODIFY_CONFIG = 58,
    REPAIR_UPDATE_SYSTEM_TABLE = 59,
    REPAIR_FLUSH_HINTS_BATCHLOG = 60,
    MAPREDUCE_REQUEST = 61,
    GET_GROUP0_UPGRADE_STATE = 62,
    DIRECT_FD_PING = 63,
    RAFT_TOPOLOGY_CMD = 64,
    RAFT_PULL_SNAPSHOT = 65,
    TABLET_STREAM_DATA = 66,
    TABLET_CLEANUP = 67,
    JOIN_NODE_REQUEST = 68,
    JOIN_NODE_RESPONSE = 69,
    TABLET_STREAM_FILES = 70,
    STREAM_BLOB = 71,
    TABLE_LOAD_STATS = 72,
    JOIN_NODE_QUERY = 73,
    TASKS_GET_CHILDREN = 74,
    LAST = 75,
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

    // We (ab)use `MIGRATION_REQUEST` verb to transfer raft group 0 snapshots,
    // which contain additional data (besides schema tables mutations).
    // When used inside group 0 snapshot transfer, this is `true`.
    bool group0_snapshot_transfer = false;
};

class messaging_service : public seastar::async_sharded_service<messaging_service>, public peering_sharded_service<messaging_service> {
public:
    struct rpc_protocol_wrapper;
    struct rpc_protocol_client_wrapper;
    struct rpc_protocol_server_wrapper;
    struct shard_info;

    using msg_addr = netw::msg_addr;
    using inet_address = gms::inet_address;
    using clients_map = std::unordered_map<msg_addr, shard_info, msg_addr::hash>;

    // This should change only if serialization format changes
    static constexpr int32_t current_version = 0;

    struct shard_info {
        shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client, bool topology_ignored);
        shared_ptr<rpc_protocol_client_wrapper> rpc_client;
        const bool topology_ignored;
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

    struct config {
        locator::host_id id;
        gms::inet_address ip;                   // a.k.a. listen_address - the address this node is listening on
        gms::inet_address broadcast_address;    // This node's address, as told to other nodes
        uint16_t port;
        uint16_t ssl_port = 0;
        encrypt_what encrypt = encrypt_what::none;
        compress_what compress = compress_what::none;
        tcp_nodelay_what tcp_nodelay = tcp_nodelay_what::all;
        bool listen_on_broadcast_address = false;
        size_t rpc_memory_limit = 1'000'000;
        std::unordered_map<gms::inet_address, gms::inet_address> preferred_ips;
        maintenance_mode_enabled maintenance_mode = maintenance_mode_enabled::no;
    };

    struct scheduling_config {
        struct tenant {
            scheduling_group sched_group;
            sstring name;
        };
        // Must have at least one element. No two tenants should have the same
        // scheduling group. [0] is the default tenant, that all unknown
        // scheduling groups will fall back to. The default tenant should use
        // the statement scheduling group, for backward compatibility. In fact
        // any other scheduling group would be dropped as the default tenant,
        // does not transfer its scheduling group across the wire.
        std::vector<tenant> statement_tenants;
        scheduling_group streaming;
        scheduling_group gossip;
    };

private:
    struct scheduling_info_for_connection_index {
        scheduling_group sched_group;
        sstring isolation_cookie;
    };
    struct tenant_connection_index {
        scheduling_group sched_group;
        unsigned cliend_idx;
    };
private:
    config _cfg;
    locator::shared_token_metadata* _token_metadata = nullptr;
    // map: Node broadcast address -> Node internal IP, and the reversed mapping, for communication within the same data center
    std::unordered_map<gms::inet_address, gms::inet_address> _preferred_ip_cache, _preferred_to_endpoint;
    std::unique_ptr<rpc_protocol_wrapper> _rpc;
    std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;
    std::unique_ptr<seastar::tls::credentials_builder> _credentials_builder;
    std::array<std::unique_ptr<rpc_protocol_server_wrapper>, 2> _server_tls;
    std::vector<clients_map> _clients;
    uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::LAST)] = {};
    bool _shutting_down = false;
    connection_drop_signal_t _connection_dropped;
    scheduling_config _scheduling_config;
    std::vector<scheduling_info_for_connection_index> _scheduling_info_for_connection_index;
    std::vector<tenant_connection_index> _connection_index_for_tenant;

    struct connection_ref;
    std::unordered_multimap<locator::host_id, connection_ref> _host_connections;
    std::unordered_set<locator::host_id> _banned_hosts;

    future<> shutdown_tls_server();
    future<> shutdown_nontls_server();
    future<> stop_tls_server();
    future<> stop_nontls_server();
    future<> stop_client();
    void init_local_preferred_ip_cache(const std::unordered_map<gms::inet_address, gms::inet_address>& ips_cache);
public:
    using clock_type = lowres_clock;

    messaging_service(locator::host_id id, gms::inet_address ip, uint16_t port);
    messaging_service(config cfg, scheduling_config scfg, std::shared_ptr<seastar::tls::credentials_builder>);
    ~messaging_service();

    future<> start();
    future<> start_listen(locator::shared_token_metadata& stm);
    uint16_t port() const noexcept {
        return _cfg.port;
    }
    gms::inet_address listen_address() const noexcept {
        return _cfg.ip;
    }
    gms::inet_address broadcast_address() const noexcept {
        return _cfg.broadcast_address;
    }
    future<> shutdown();
    future<> stop();
    static rpc::no_wait_type no_wait();
    bool is_shutting_down() { return _shutting_down; }
    gms::inet_address get_preferred_ip(gms::inet_address ep);
    void cache_preferred_ip(gms::inet_address ep, gms::inet_address ip);
    gms::inet_address get_public_endpoint_for(const gms::inet_address&) const;

    future<> unregister_handler(messaging_verb verb);

    // Wrapper for PREPARE_MESSAGE verb
    void register_prepare_message(std::function<future<streaming::prepare_message> (const rpc::client_info& cinfo,
            streaming::prepare_message msg, streaming::plan_id plan_id, sstring description, rpc::optional<streaming::stream_reason> reason, rpc::optional<service::session_id>)>&& func);
    future<streaming::prepare_message> send_prepare_message(msg_addr id, streaming::prepare_message msg, streaming::plan_id plan_id,
            sstring description, streaming::stream_reason, service::session_id);
    future<> unregister_prepare_message();

    // Wrapper for PREPARE_DONE_MESSAGE verb
    void register_prepare_done_message(std::function<future<> (const rpc::client_info& cinfo, streaming::plan_id plan_id, unsigned dst_cpu_id)>&& func);
    future<> send_prepare_done_message(msg_addr id, streaming::plan_id plan_id, unsigned dst_cpu_id);
    future<> unregister_prepare_done_message();

    // Wrapper for STREAM_MUTATION_FRAGMENTS
    // The receiver of STREAM_MUTATION_FRAGMENTS sends status code to the sender to notify any error on the receiver side. The status code is of type int32_t. 0 means successful, -1 means error, -2 means error and table is dropped, other status code value are reserved for future use.
    void register_stream_mutation_fragments(std::function<future<rpc::sink<int32_t>> (const rpc::client_info& cinfo, streaming::plan_id plan_id, table_schema_version schema_id, table_id cf_id, uint64_t estimated_partitions, rpc::optional<streaming::stream_reason> reason_opt, rpc::source<frozen_mutation_fragment, rpc::optional<streaming::stream_mutation_fragments_cmd>> source, rpc::optional<service::session_id>)>&& func);
    future<> unregister_stream_mutation_fragments();
    rpc::sink<int32_t> make_sink_for_stream_mutation_fragments(rpc::source<frozen_mutation_fragment, rpc::optional<streaming::stream_mutation_fragments_cmd>>& source);
    future<std::tuple<rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, rpc::source<int32_t>>> make_sink_and_source_for_stream_mutation_fragments(table_schema_version schema_id, streaming::plan_id plan_id, table_id cf_id, uint64_t estimated_partitions, streaming::stream_reason reason, service::session_id session, msg_addr id);

    // Wrapper for REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM
    future<std::tuple<rpc::sink<repair_hash_with_cmd>, rpc::source<repair_row_on_wire_with_cmd>>> make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, msg_addr id);
    rpc::sink<repair_row_on_wire_with_cmd> make_sink_for_repair_get_row_diff_with_rpc_stream(rpc::source<repair_hash_with_cmd>& source);
    void register_repair_get_row_diff_with_rpc_stream(std::function<future<rpc::sink<repair_row_on_wire_with_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_hash_with_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt)>&& func);
    future<> unregister_repair_get_row_diff_with_rpc_stream();

    // Wrapper for REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM
    future<std::tuple<rpc::sink<repair_row_on_wire_with_cmd>, rpc::source<repair_stream_cmd>>> make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, msg_addr id);
    rpc::sink<repair_stream_cmd> make_sink_for_repair_put_row_diff_with_rpc_stream(rpc::source<repair_row_on_wire_with_cmd>& source);
    void register_repair_put_row_diff_with_rpc_stream(std::function<future<rpc::sink<repair_stream_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_row_on_wire_with_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt)>&& func);
    future<> unregister_repair_put_row_diff_with_rpc_stream();

    // Wrapper for REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM
    future<std::tuple<rpc::sink<repair_stream_cmd>, rpc::source<repair_hash_with_cmd>>> make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, msg_addr id);
    rpc::sink<repair_hash_with_cmd> make_sink_for_repair_get_full_row_hashes_with_rpc_stream(rpc::source<repair_stream_cmd>& source);
    void register_repair_get_full_row_hashes_with_rpc_stream(std::function<future<rpc::sink<repair_hash_with_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_stream_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt)>&& func);
    future<> unregister_repair_get_full_row_hashes_with_rpc_stream();

    void register_stream_mutation_done(std::function<future<> (const rpc::client_info& cinfo, streaming::plan_id plan_id, dht::token_range_vector ranges, table_id cf_id, unsigned dst_cpu_id)>&& func);
    future<> send_stream_mutation_done(msg_addr id, streaming::plan_id plan_id, dht::token_range_vector ranges, table_id cf_id, unsigned dst_cpu_id);
    future<> unregister_stream_mutation_done();

    void register_complete_message(std::function<future<> (const rpc::client_info& cinfo, streaming::plan_id plan_id, unsigned dst_cpu_id, rpc::optional<bool> failed)>&& func);
    future<> send_complete_message(msg_addr id, streaming::plan_id plan_id, unsigned dst_cpu_id, bool failed = false);
    future<> unregister_complete_message();

    // Wrapper for REPAIR_GET_FULL_ROW_HASHES
    void register_repair_get_full_row_hashes(std::function<future<repair_hash_set> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_get_full_row_hashes();
    future<repair_hash_set> send_repair_get_full_row_hashes(msg_addr id, uint32_t repair_meta_id, shard_id dst_cpu_id);

    // Wrapper for REPAIR_GET_COMBINED_ROW_HASH
    void register_repair_get_combined_row_hash(std::function<future<get_combined_row_hash_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_get_combined_row_hash();
    future<get_combined_row_hash_response> send_repair_get_combined_row_hash(msg_addr id, uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary, shard_id dst_cpu_id);

    // Wrapper for REPAIR_GET_SYNC_BOUNDARY
    void register_repair_get_sync_boundary(std::function<future<get_sync_boundary_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_get_sync_boundary();
    future<get_sync_boundary_response> send_repair_get_sync_boundary(msg_addr id, uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary, shard_id dst_cpu_id);

    // Wrapper for REPAIR_GET_ROW_DIFF
    void register_repair_get_row_diff(std::function<future<repair_rows_on_wire> (const rpc::client_info& cinfo, uint32_t repair_meta_id, repair_hash_set set_diff, bool needs_all_rows, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_get_row_diff();
    future<repair_rows_on_wire> send_repair_get_row_diff(msg_addr id, uint32_t repair_meta_id, repair_hash_set set_diff, bool needs_all_rows, shard_id dst_cpu_id);

    // Wrapper for REPAIR_PUT_ROW_DIFF
    void register_repair_put_row_diff(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, repair_rows_on_wire row_diff, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_put_row_diff();
    future<> send_repair_put_row_diff(msg_addr id, uint32_t repair_meta_id, repair_rows_on_wire row_diff, shard_id dst_cpu_id);

    // Wrapper for REPAIR_ROW_LEVEL_START
    void register_repair_row_level_start(std::function<future<repair_row_level_start_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, rpc::optional<streaming::stream_reason> reason, rpc::optional<gc_clock::time_point> compaction_time, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_row_level_start();
    future<rpc::optional<repair_row_level_start_response>> send_repair_row_level_start(msg_addr id, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, streaming::stream_reason reason, gc_clock::time_point compaction_time, shard_id dst_cpu_id);

    // Wrapper for REPAIR_ROW_LEVEL_STOP
    void register_repair_row_level_stop(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_row_level_stop();
    future<> send_repair_row_level_stop(msg_addr id, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, shard_id dst_cpu_id);

    // Wrapper for REPAIR_GET_ESTIMATED_PARTITIONS
    void register_repair_get_estimated_partitions(std::function<future<uint64_t> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_get_estimated_partitions();
    future<uint64_t> send_repair_get_estimated_partitions(msg_addr id, uint32_t repair_meta_id, shard_id dst_cpu_id);

    // Wrapper for REPAIR_SET_ESTIMATED_PARTITIONS
    void register_repair_set_estimated_partitions(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, uint64_t estimated_partitions, rpc::optional<shard_id> dst_cpu_id)>&& func);
    future<> unregister_repair_set_estimated_partitions();
    future<> send_repair_set_estimated_partitions(msg_addr id, uint32_t repair_meta_id, uint64_t estimated_partitions, shard_id dst_cpu_id);

    // Wrapper for REPAIR_GET_DIFF_ALGORITHMS
    void register_repair_get_diff_algorithms(std::function<future<std::vector<row_level_diff_detect_algorithm>> (const rpc::client_info& cinfo)>&& func);
    future<> unregister_repair_get_diff_algorithms();
    future<std::vector<row_level_diff_detect_algorithm>> send_repair_get_diff_algorithms(msg_addr id);

    // Wrapper for NODE_OPS_CMD
    void register_node_ops_cmd(std::function<future<node_ops_cmd_response> (const rpc::client_info& cinfo, node_ops_cmd_request)>&& func);
    future<> unregister_node_ops_cmd();
    future<node_ops_cmd_response> send_node_ops_cmd(msg_addr id, node_ops_cmd_request);

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
    future<rpc::tuple<std::vector<frozen_mutation>, rpc::optional<std::vector<canonical_mutation>>>> send_migration_request(msg_addr id,
            abort_source& as, schema_pull_options options);

    // Wrapper for GET_SCHEMA_VERSION
    void register_get_schema_version(std::function<future<frozen_schema>(unsigned, table_schema_version)>&& func);
    future<> unregister_get_schema_version();
    future<frozen_schema> send_get_schema_version(msg_addr, table_schema_version);

    // Wrapper for SCHEMA_CHECK
    void register_schema_check(std::function<future<table_schema_version>()>&& func);
    future<> unregister_schema_check();
    future<table_schema_version> send_schema_check(msg_addr);
    future<table_schema_version> send_schema_check(msg_addr, abort_source&);

    // Wrapper for TASKS_GET_CHILDREN
    void register_tasks_get_children(std::function<future<tasks::get_children_response> (const rpc::client_info& cinfo, tasks::get_children_request)>&& func);
    future<> unregister_tasks_get_children();
    future<tasks::get_children_response> send_tasks_get_children(msg_addr id, tasks::get_children_request);

    void foreach_server_connection_stats(std::function<void(const rpc::client_info&, const rpc::stats&)>&& f) const;

    // Drops all connections from the given host and prevents further communication from it to happen.
    //
    // No further RPC handlers will be called for that node,
    // but we don't prevent handlers that were started concurrently from finishing.
    future<> ban_host(locator::host_id);
private:
    template <typename Fn>
    requires std::is_invocable_r_v<bool, Fn, const shard_info&>
    void find_and_remove_client(clients_map& clients, msg_addr id, Fn&& filter);
    void do_start_listen();

    bool topology_known_for(inet_address) const;
    bool is_same_dc(inet_address ep) const;
    bool is_same_rack(inet_address ep) const;

    bool is_host_banned(locator::host_id);

    sstring client_metrics_domain(unsigned idx, inet_address addr) const;

public:
    // Return rpc::protocol::client for a shard which is a ip + cpuid pair.
    shared_ptr<rpc_protocol_client_wrapper> get_rpc_client(messaging_verb verb, msg_addr id);
    void remove_error_rpc_client(messaging_verb verb, msg_addr id);
    void remove_rpc_client_with_ignored_topology(msg_addr id);
    void remove_rpc_client(msg_addr id);
    connection_drop_registration_t when_connection_drops(connection_drop_slot_t& slot) {
        return _connection_dropped.connect(slot);
    }
    std::unique_ptr<rpc_protocol_wrapper>& rpc();
    static msg_addr get_source(const rpc::client_info& client);
    scheduling_group scheduling_group_for_verb(messaging_verb verb) const;
    scheduling_group scheduling_group_for_isolation_cookie(const sstring& isolation_cookie) const;
    std::vector<messaging_service::scheduling_info_for_connection_index> initial_scheduling_info() const;
    unsigned get_rpc_client_idx(messaging_verb verb) const;
    static constexpr std::array<std::string_view, 3> _connection_types_prefix = {"statement:", "statement-ack:", "forward:"}; // "forward" is the old name for "mapreduce"
};

} // namespace netw
