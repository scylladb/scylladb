/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>

#include "message/messaging_service.hh"
#include <seastar/core/distributed.hh>
#include "gms/gossiper.hh"
#include "streaming/prepare_message.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "query-result.hh"
#include <seastar/rpc/rpc.hh>
#include "mutation/canonical_mutation.hh"
#include "db/config.hh"
#include "db/view/view_update_backlog.hh"
#include "dht/i_partitioner.hh"
#include "interval.hh"
#include "frozen_schema.hh"
#include "repair/repair.hh"
#include "node_ops/node_ops_ctl.hh"
#include "service/paxos/proposal.hh"
#include "service/paxos/prepare_response.hh"
#include "query-request.hh"
#include "mutation_query.hh"
#include "repair/repair.hh"
#include "streaming/stream_reason.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "cache_temperature.hh"
#include "raft/raft.hh"
#include "service/raft/group0_fwd.hh"
#include "replica/exceptions.hh"
#include "serializer.hh"
#include "db/per_partition_rate_limit_info.hh"
#include "service/topology_state_machine.hh"
#include "service/raft/join_node.hh"
#include "idl/consistency_level.dist.hh"
#include "idl/tracing.dist.hh"
#include "idl/result.dist.hh"
#include "idl/reconcilable_result.dist.hh"
#include "idl/ring_position.dist.hh"
#include "idl/keys.dist.hh"
#include "idl/uuid.dist.hh"
#include "idl/frozen_mutation.dist.hh"
#include "idl/frozen_schema.dist.hh"
#include "idl/streaming.dist.hh"
#include "idl/token.dist.hh"
#include "idl/gossip_digest.dist.hh"
#include "idl/read_command.dist.hh"
#include "idl/range.dist.hh"
#include "idl/position_in_partition.dist.hh"
#include "idl/partition_checksum.dist.hh"
#include "idl/query.dist.hh"
#include "idl/cache_temperature.dist.hh"
#include "idl/view.dist.hh"
#include "idl/mutation.dist.hh"
#include "idl/messaging_service.dist.hh"
#include "idl/paxos.dist.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft.dist.hh"
#include "idl/group0.dist.hh"
#include "idl/replica_exception.dist.hh"
#include "idl/per_partition_rate_limit_info.dist.hh"
#include "idl/storage_proxy.dist.hh"
#include "idl/gossip.dist.hh"
#include "idl/storage_service.dist.hh"
#include "idl/join_node.dist.hh"
#include "message/rpc_protocol_impl.hh"
#include "idl/consistency_level.dist.impl.hh"
#include "idl/tracing.dist.impl.hh"
#include "idl/result.dist.impl.hh"
#include "idl/reconcilable_result.dist.impl.hh"
#include "idl/ring_position.dist.impl.hh"
#include "idl/keys.dist.impl.hh"
#include "idl/uuid.dist.impl.hh"
#include "idl/frozen_mutation.dist.impl.hh"
#include "idl/frozen_schema.dist.impl.hh"
#include "idl/streaming.dist.impl.hh"
#include "idl/token.dist.impl.hh"
#include "idl/gossip_digest.dist.impl.hh"
#include "idl/read_command.dist.impl.hh"
#include "idl/range.dist.impl.hh"
#include "idl/position_in_partition.dist.impl.hh"
#include "idl/query.dist.impl.hh"
#include "idl/cache_temperature.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"
#include "idl/messaging_service.dist.impl.hh"
#include "idl/paxos.dist.impl.hh"
#include "idl/raft_storage.dist.impl.hh"
#include "idl/raft.dist.impl.hh"
#include "idl/group0.dist.impl.hh"
#include "idl/view.dist.impl.hh"
#include "idl/replica_exception.dist.impl.hh"
#include "idl/per_partition_rate_limit_info.dist.impl.hh"
#include "idl/storage_proxy.dist.impl.hh"
#include "idl/gossip.dist.impl.hh"
#include <seastar/rpc/lz4_compressor.hh>
#include <seastar/rpc/lz4_fragmented_compressor.hh>
#include <seastar/rpc/multi_algo_compressor_factory.hh>
#include "partition_range_compat.hh"
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include "mutation/frozen_mutation.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "idl/partition_checksum.dist.impl.hh"
#include "idl/mapreduce_request.dist.hh"
#include "idl/mapreduce_request.dist.impl.hh"
#include "idl/storage_service.dist.impl.hh"
#include "idl/join_node.dist.impl.hh"

namespace netw {

static_assert(!std::is_default_constructible_v<msg_addr>);
static_assert(std::is_nothrow_copy_constructible_v<msg_addr>);
static_assert(std::is_nothrow_move_constructible_v<msg_addr>);

static logging::logger mlogger("messaging_service");
static logging::logger rpc_logger("rpc");

using inet_address = gms::inet_address;
using gossip_digest_syn = gms::gossip_digest_syn;
using gossip_digest_ack = gms::gossip_digest_ack;
using gossip_digest_ack2 = gms::gossip_digest_ack2;
using namespace std::chrono_literals;

static rpc::lz4_fragmented_compressor::factory lz4_fragmented_compressor_factory;
static rpc::lz4_compressor::factory lz4_compressor_factory;
static rpc::multi_algo_compressor_factory compressor_factory {
    &lz4_fragmented_compressor_factory,
    &lz4_compressor_factory,
};

struct messaging_service::rpc_protocol_server_wrapper : public rpc_protocol::server { using rpc_protocol::server::server; };

struct messaging_service::connection_ref {
    // Refers to one of the servers in `messaging_service::_server` or `messaging_service::_server_tls`.
    rpc::server& server;

    // Refers to a connection inside `server`.
    rpc::connection_id conn_id;
};

constexpr int32_t messaging_service::current_version;

// Count of connection types that are not associated with any tenant
const size_t PER_SHARD_CONNECTION_COUNT = 2;
// Counts per tenant connection types
const size_t PER_TENANT_CONNECTION_COUNT = 3;

bool operator==(const msg_addr& x, const msg_addr& y) noexcept {
    // Ignore cpu id for now since we do not really support shard to shard connections
    return x.addr == y.addr;
}

bool operator<(const msg_addr& x, const msg_addr& y) noexcept {
    // Ignore cpu id for now since we do not really support shard to shard connections
    if (x.addr < y.addr) {
        return true;
    } else {
        return false;
    }
}

size_t msg_addr::hash::operator()(const msg_addr& id) const noexcept {
    // Ignore cpu id for now since we do not really support // shard to shard connections
    return std::hash<bytes_view>()(id.addr.bytes());
}

messaging_service::shard_info::shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client, bool topo_ignored)
    : rpc_client(std::move(client))
    , topology_ignored(topo_ignored)
{
}

rpc::stats messaging_service::shard_info::get_stats() const {
    return rpc_client->get_stats();
}

void messaging_service::foreach_client(std::function<void(const msg_addr& id, const shard_info& info)> f) const {
    for (unsigned idx = 0; idx < _clients.size(); idx ++) {
        for (auto i = _clients[idx].cbegin(); i != _clients[idx].cend(); i++) {
            f(i->first, i->second);
        }
    }
}

void messaging_service::foreach_server_connection_stats(std::function<void(const rpc::client_info&, const rpc::stats&)>&& f) const {
    for (auto&& s : _server) {
        if (s) {
            s->foreach_connection([f](const rpc_protocol::server::connection& c) {
                f(c.info(), c.get_stats());
            });
        }
    }
}

void messaging_service::increment_dropped_messages(messaging_verb verb) {
    _dropped_messages[static_cast<int32_t>(verb)]++;
}

uint64_t messaging_service::get_dropped_messages(messaging_verb verb) const {
    return _dropped_messages[static_cast<int32_t>(verb)];
}

const uint64_t* messaging_service::get_dropped_messages() const {
    return _dropped_messages;
}

int32_t messaging_service::get_raw_version(const gms::inet_address& endpoint) const {
    // FIXME: messaging service versioning
    return current_version;
}

bool messaging_service::knows_version(const gms::inet_address& endpoint) const {
    // FIXME: messaging service versioning
    return true;
}

future<> messaging_service::unregister_handler(messaging_verb verb) {
    return _rpc->unregister_handler(verb);
}

messaging_service::messaging_service(locator::host_id id, gms::inet_address ip, uint16_t port)
    : messaging_service(config{std::move(id), ip, ip, port},
                        scheduling_config{{{{}, "$default"}}, {}, {}}, nullptr)
{}

static
rpc::resource_limits
rpc_resource_limits(size_t memory_limit) {
    rpc::resource_limits limits;
    limits.bloat_factor = 3;
    limits.basic_request_size = 1000;
    limits.max_memory = memory_limit;
    return limits;
}

future<> messaging_service::start() {
    if (_credentials_builder && !_credentials) {
        return _credentials_builder->build_reloadable_server_credentials([](const std::unordered_set<sstring>& files, std::exception_ptr ep) {
            if (ep) {
                mlogger.warn("Exception loading {}: {}", files, ep);
            } else {
                mlogger.info("Reloaded {}", files);
            }
        }).then([this](shared_ptr<seastar::tls::server_credentials> creds) {
            _credentials = std::move(creds);
        });
    }
    return make_ready_future<>();
}

future<> messaging_service::start_listen(locator::shared_token_metadata& stm) {
    _token_metadata = &stm;
    do_start_listen();
    return make_ready_future<>();
}

bool messaging_service::topology_known_for(inet_address addr) const {
    // The token metadata pointer is nullptr before
    // the service is start_listen()-ed and after it's being shutdown()-ed.
    return _token_metadata
        && _token_metadata->get()->get_topology().has_endpoint(addr);
}

// Precondition: `topology_known_for(addr)`.
bool messaging_service::is_same_dc(inet_address addr) const {
    const auto& topo = _token_metadata->get()->get_topology();
    return topo.get_datacenter(addr) == topo.get_datacenter();
}

// Precondition: `topology_known_for(addr)`.
bool messaging_service::is_same_rack(inet_address addr) const {
    const auto& topo = _token_metadata->get()->get_topology();
    return topo.get_rack(addr) == topo.get_rack();
}

// The socket metrics domain defines the way RPC metrics are grouped
// for different sockets. Thus, the domain includes:
//
// - Target datacenter name, because it's pointless to merge networking
//   statis for connections that are in advance known to have different
//   timings and rates
// - The verb-idx to tell different RPC channels from each other. For
//   that the isolation cookie suits very well, because these cookies
//   are different for different indices and are more informative than
//   plain numbers
sstring messaging_service::client_metrics_domain(unsigned idx, inet_address addr) const {
    sstring ret = _scheduling_info_for_connection_index[idx].isolation_cookie;
    if (_token_metadata) {
        const auto& topo = _token_metadata->get()->get_topology();
        if (topo.has_endpoint(addr)) {
            ret += ":" + topo.get_datacenter(addr);
        }
    }
    return ret;
}

future<> messaging_service::ban_host(locator::host_id id) {
    return container().invoke_on_all([id] (messaging_service& ms) {
        if (ms._banned_hosts.contains(id) || ms.is_shutting_down()) {
            return;
        }

        ms._banned_hosts.insert(id);
        auto [start, end] = ms._host_connections.equal_range(id);
        for (auto it = start; it != end; ++it) {
            auto& conn_ref = it->second;
            conn_ref.server.abort_connection(conn_ref.conn_id);
        }
        ms._host_connections.erase(start, end);
    });
}

bool messaging_service::is_host_banned(locator::host_id id) {
    return _banned_hosts.contains(id);
}

void messaging_service::do_start_listen() {
    auto broadcast_address = this->broadcast_address();
    bool listen_to_bc = _cfg.listen_on_broadcast_address && _cfg.ip != broadcast_address;
    rpc::server_options so;
    if (_cfg.compress != compress_what::none) {
        so.compressor_factory = &compressor_factory;
    }
    so.load_balancing_algorithm = server_socket::load_balancing_algorithm::port;

    // FIXME: we don't set so.tcp_nodelay, because we can't tell at this point whether the connection will come from a
    //        local or remote datacenter, and whether or not the connection will be used for gossip. We can fix
    //        the first by wrapping its server_socket, but not the second.
    auto limits = rpc_resource_limits(_cfg.rpc_memory_limit);
    limits.isolate_connection = [this] (sstring isolation_cookie) {
        rpc::isolation_config cfg;
        cfg.sched_group = scheduling_group_for_isolation_cookie(isolation_cookie);
        return cfg;
    };
    if (!_server[0] && _cfg.encrypt != encrypt_what::all && _cfg.port) {
        auto listen = [&] (const gms::inet_address& a, rpc::streaming_domain_type sdomain) {
            so.streaming_domain = sdomain;
            so.filter_connection = {};
            switch (_cfg.encrypt) {
                default:
                case encrypt_what::none:
                    break;
                case encrypt_what::dc:
                    so.filter_connection = [this](const seastar::socket_address& caddr) {
                        auto addr = get_public_endpoint_for(caddr);
                        return topology_known_for(addr) && is_same_dc(addr);
                    };
                    break;
                case encrypt_what::rack:
                    so.filter_connection = [this](const seastar::socket_address& caddr) {
                        auto addr = get_public_endpoint_for(caddr);
                        return topology_known_for(addr) && is_same_dc(addr) && is_same_rack(addr);
                    };
                    break;
            }
            auto addr = socket_address{a, _cfg.port};
            return std::unique_ptr<rpc_protocol_server_wrapper>(new rpc_protocol_server_wrapper(_rpc->protocol(),
                    so, addr, limits));
        };
        _server[0] = listen(_cfg.ip, rpc::streaming_domain_type(0x55AA));
        if (listen_to_bc) {
            _server[1] = listen(broadcast_address, rpc::streaming_domain_type(0x66BB));
        }
    }

    if (!_server_tls[0] && _cfg.ssl_port) {
        auto listen = [&] (const gms::inet_address& a, rpc::streaming_domain_type sdomain) {
            so.filter_connection = {};
            so.streaming_domain = sdomain;
            return std::unique_ptr<rpc_protocol_server_wrapper>(
                    [this, &so, &a, limits] () -> std::unique_ptr<rpc_protocol_server_wrapper>{
                if (_cfg.encrypt == encrypt_what::none) {
                    return nullptr;
                }
                if (!_credentials) {
                    throw std::invalid_argument("No certificates specified for encrypted service");
                }
                listen_options lo;
                lo.reuse_address = true;
                lo.lba =  server_socket::load_balancing_algorithm::port;
                auto addr = socket_address{a, _cfg.ssl_port};
                return std::make_unique<rpc_protocol_server_wrapper>(_rpc->protocol(),
                        so, seastar::tls::listen(_credentials, addr, lo), limits);
            }());
        };
        _server_tls[0] = listen(_cfg.ip, rpc::streaming_domain_type(0x77CC));
        if (listen_to_bc) {
            _server_tls[1] = listen(broadcast_address, rpc::streaming_domain_type(0x88DD));
        }
    }
    // Do this on just cpu 0, to avoid duplicate logs.
    if (this_shard_id() == 0) {
        if (_server_tls[0]) {
            mlogger.info("Starting Encrypted Messaging Service on SSL address {} port {}", _cfg.ip, _cfg.ssl_port);
        }
        if (_server_tls[1]) {
            mlogger.info("Starting Encrypted Messaging Service on SSL broadcast address {} port {}", broadcast_address, _cfg.ssl_port);
        }
        if (_server[0]) {
            mlogger.info("Starting Messaging Service on address {} port {}", _cfg.ip, _cfg.port);
        }
        if (_server[1]) {
            mlogger.info("Starting Messaging Service on broadcast address {} port {}", broadcast_address, _cfg.port);
        }
    }
}

messaging_service::messaging_service(config cfg, scheduling_config scfg, std::shared_ptr<seastar::tls::credentials_builder> credentials)
    : _cfg(std::move(cfg))
    , _rpc(new rpc_protocol_wrapper(serializer { }))
    , _credentials_builder(credentials ? std::make_unique<seastar::tls::credentials_builder>(*credentials) : nullptr)
    , _clients(PER_SHARD_CONNECTION_COUNT + scfg.statement_tenants.size() * PER_TENANT_CONNECTION_COUNT)
    , _scheduling_config(scfg)
    , _scheduling_info_for_connection_index(initial_scheduling_info())
{
    _rpc->set_logger(&rpc_logger);

    // this initialization should be done before any handler registration
    // this is because register_handler calls to: scheduling_group_for_verb
    // which in turn relies on _connection_index_for_tenant to be initialized.
    _connection_index_for_tenant.reserve(_scheduling_config.statement_tenants.size());
    for (unsigned i = 0; i <  _scheduling_config.statement_tenants.size(); ++i) {
        _connection_index_for_tenant.push_back({_scheduling_config.statement_tenants[i].sched_group, i});
    }

    register_handler(this, messaging_verb::CLIENT_ID, [this] (rpc::client_info& ci, gms::inet_address broadcast_address, uint32_t src_cpu_id, rpc::optional<uint64_t> max_result_size, rpc::optional<utils::UUID> host_id) {
        if (host_id) {
            auto peer_host_id = locator::host_id(*host_id);
            if (is_host_banned(peer_host_id)) {
                ci.server.abort_connection(ci.conn_id);
                return rpc::no_wait;
            }
            ci.attach_auxiliary("host_id", peer_host_id);
            _host_connections.emplace(peer_host_id, connection_ref {
                .server = ci.server,
                .conn_id = ci.conn_id,
            });
        }
        ci.attach_auxiliary("baddr", broadcast_address);
        ci.attach_auxiliary("src_cpu_id", src_cpu_id);
        ci.attach_auxiliary("max_result_size", max_result_size.value_or(query::result_memory_limiter::maximum_result_size));
        return rpc::no_wait;
    });

    init_local_preferred_ip_cache(_cfg.preferred_ips);
}

msg_addr messaging_service::get_source(const rpc::client_info& cinfo) {
    return msg_addr{
        cinfo.retrieve_auxiliary<gms::inet_address>("baddr"),
        cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id")
    };
}

messaging_service::~messaging_service() = default;

static future<> do_with_servers(std::string_view what, std::array<std::unique_ptr<messaging_service::rpc_protocol_server_wrapper>, 2>& servers, auto method) {
    mlogger.info("{} server", what);
    co_await coroutine::parallel_for_each(
            servers | boost::adaptors::filtered([] (auto& ptr) { return bool(ptr); }) | boost::adaptors::indirected,
            method);
    mlogger.info("{} server - Done", what);
}

future<> messaging_service::shutdown_tls_server() {
    return do_with_servers("Shutting down tls", _server_tls, std::mem_fn(&rpc_protocol_server_wrapper::shutdown));
}

future<> messaging_service::shutdown_nontls_server() {
    return do_with_servers("Shutting down nontls", _server, std::mem_fn(&rpc_protocol_server_wrapper::shutdown));
}

future<> messaging_service::stop_tls_server() {
    return do_with_servers("Stopping tls", _server_tls, std::mem_fn(&rpc_protocol_server_wrapper::stop));
}

future<> messaging_service::stop_nontls_server() {
    return do_with_servers("Stopping nontls", _server, std::mem_fn(&rpc_protocol_server_wrapper::stop));
}

future<> messaging_service::stop_client() {
    return parallel_for_each(_clients, [] (auto& m) {
        return parallel_for_each(m, [] (std::pair<const msg_addr, shard_info>& c) {
            mlogger.info("Stopping client for address: {}", c.first);
            return c.second.rpc_client->stop().then([addr = c.first] {
                mlogger.info("Stopping client for address: {} - Done", addr);
            });
        }).finally([&m] {
            // no new clients should be added by get_rpc_client(), as it
            // asserts that _shutting_down is true
            m.clear();
            mlogger.info("Stopped clients");
        });
    });
}

future<> messaging_service::shutdown() {
    _shutting_down = true;
    co_await when_all(shutdown_nontls_server(), shutdown_tls_server(), stop_client()).discard_result();
    _token_metadata = nullptr;
}

future<> messaging_service::stop() {
    if (!_shutting_down) {
        co_await shutdown();
    }
    co_await when_all(stop_nontls_server(), stop_tls_server());
    co_await unregister_handler(messaging_verb::CLIENT_ID);
    if (_rpc->has_handlers()) {
        mlogger.error("RPC server still has handlers registered");
        for (auto verb = messaging_verb::MUTATION; verb < messaging_verb::LAST;
                verb = messaging_verb(int(verb) + 1)) {
            if (_rpc->has_handler(verb)) {
                mlogger.error(" - {}", static_cast<int>(verb));
            }
        }

        std::abort();
    }
}

rpc::no_wait_type messaging_service::no_wait() {
    return rpc::no_wait;
}

// The verbs using this RPC client use the following connection settings,
// regardless of whether the peer is in the same DC/Rack or not:
// - tcp_nodelay
// - encryption (unless completely disabled in config)
// - compression (unless completely disabled in config)
//
// The reason for having topology-independent setting for encryption is to ensure
// that gossiper verbs can reach the peer, even though the peer may not know our topology yet.
// See #11992 for detailed explanations.
//
// We also always want `tcp_nodelay` for gossiper verbs so they have low latency
// (and there's no advantage from batching verbs in this group anyway).
//
// And since we fixed a topology-independent setting for encryption and tcp_nodelay,
// to keep things simple, we also fix a setting for compression. This allows this RPC client
// to be established without checking the topology (which may not be known anyway
// when we first start gossiping).
static constexpr unsigned TOPOLOGY_INDEPENDENT_IDX = 0;

static constexpr unsigned do_get_rpc_client_idx(messaging_verb verb) {
    // *_CONNECTION_COUNT constants needs to be updated after allocating a new index.
    switch (verb) {
    // GET_SCHEMA_VERSION is sent from read/mutate verbs so should be
    // sent on a different connection to avoid potential deadlocks
    // as well as reduce latency as there are potentially many requests
    // blocked on schema version request.
    case messaging_verb::GOSSIP_DIGEST_SYN:
    case messaging_verb::GOSSIP_DIGEST_ACK:
    case messaging_verb::GOSSIP_DIGEST_ACK2:
    case messaging_verb::GOSSIP_SHUTDOWN:
    case messaging_verb::GOSSIP_ECHO:
    case messaging_verb::GOSSIP_GET_ENDPOINT_STATES:
    case messaging_verb::GET_SCHEMA_VERSION:
    // Raft peer exchange is mainly running at boot, but still
    // should not be blocked by any data requests.
    case messaging_verb::GROUP0_PEER_EXCHANGE:
    case messaging_verb::GROUP0_MODIFY_CONFIG:
    case messaging_verb::GET_GROUP0_UPGRADE_STATE:
    case messaging_verb::RAFT_TOPOLOGY_CMD:
    case messaging_verb::JOIN_NODE_REQUEST:
    case messaging_verb::JOIN_NODE_RESPONSE:
    case messaging_verb::JOIN_NODE_QUERY:
    case messaging_verb::TASKS_GET_CHILDREN:
        // See comment above `TOPOLOGY_INDEPENDENT_IDX`.
        // DO NOT put any 'hot' (e.g. data path) verbs in this group,
        // only verbs which are 'rare' and 'cheap'.
        // DO NOT move GOSSIP_ verbs outside this group.
        static_assert(TOPOLOGY_INDEPENDENT_IDX == 0);
        return 0;
    case messaging_verb::PREPARE_MESSAGE:
    case messaging_verb::PREPARE_DONE_MESSAGE:
    case messaging_verb::UNUSED__STREAM_MUTATION:
    case messaging_verb::STREAM_MUTATION_DONE:
    case messaging_verb::COMPLETE_MESSAGE:
    case messaging_verb::UNUSED__REPLICATION_FINISHED:
    case messaging_verb::UNUSED__REPAIR_CHECKSUM_RANGE:
    case messaging_verb::STREAM_MUTATION_FRAGMENTS:
    case messaging_verb::STREAM_BLOB:
    case messaging_verb::REPAIR_ROW_LEVEL_START:
    case messaging_verb::REPAIR_ROW_LEVEL_STOP:
    case messaging_verb::REPAIR_GET_FULL_ROW_HASHES:
    case messaging_verb::REPAIR_GET_COMBINED_ROW_HASH:
    case messaging_verb::REPAIR_GET_SYNC_BOUNDARY:
    case messaging_verb::REPAIR_GET_ROW_DIFF:
    case messaging_verb::REPAIR_PUT_ROW_DIFF:
    case messaging_verb::REPAIR_GET_ESTIMATED_PARTITIONS:
    case messaging_verb::REPAIR_SET_ESTIMATED_PARTITIONS:
    case messaging_verb::REPAIR_GET_DIFF_ALGORITHMS:
    case messaging_verb::REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM:
    case messaging_verb::REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM:
    case messaging_verb::REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM:
    case messaging_verb::REPAIR_UPDATE_SYSTEM_TABLE:
    case messaging_verb::REPAIR_FLUSH_HINTS_BATCHLOG:
    case messaging_verb::NODE_OPS_CMD:
    case messaging_verb::HINT_MUTATION:
    case messaging_verb::TABLET_STREAM_FILES:
    case messaging_verb::TABLET_STREAM_DATA:
    case messaging_verb::TABLET_CLEANUP:
    case messaging_verb::TABLE_LOAD_STATS:
        return 1;
    case messaging_verb::CLIENT_ID:
    case messaging_verb::MUTATION:
    case messaging_verb::READ_DATA:
    case messaging_verb::READ_MUTATION_DATA:
    case messaging_verb::READ_DIGEST:
    case messaging_verb::DEFINITIONS_UPDATE:
    case messaging_verb::TRUNCATE:
    case messaging_verb::MIGRATION_REQUEST:
    case messaging_verb::SCHEMA_CHECK:
    case messaging_verb::COUNTER_MUTATION:
    // Use the same RPC client for light weight transaction
    // protocol steps as for standard mutations and read requests.
    case messaging_verb::PAXOS_PREPARE:
    case messaging_verb::PAXOS_ACCEPT:
    case messaging_verb::PAXOS_LEARN:
    case messaging_verb::PAXOS_PRUNE:
    case messaging_verb::RAFT_SEND_SNAPSHOT:
    case messaging_verb::RAFT_APPEND_ENTRIES:
    case messaging_verb::RAFT_APPEND_ENTRIES_REPLY:
    case messaging_verb::RAFT_VOTE_REQUEST:
    case messaging_verb::RAFT_VOTE_REPLY:
    case messaging_verb::RAFT_TIMEOUT_NOW:
    case messaging_verb::RAFT_READ_QUORUM:
    case messaging_verb::RAFT_READ_QUORUM_REPLY:
    case messaging_verb::RAFT_EXECUTE_READ_BARRIER_ON_LEADER:
    case messaging_verb::RAFT_ADD_ENTRY:
    case messaging_verb::RAFT_MODIFY_CONFIG:
    case messaging_verb::DIRECT_FD_PING:
    case messaging_verb::RAFT_PULL_SNAPSHOT:
        return 2;
    case messaging_verb::MUTATION_DONE:
    case messaging_verb::MUTATION_FAILED:
        return 3;
    case messaging_verb::MAPREDUCE_REQUEST:
        return 4;
    case messaging_verb::LAST:
        return -1; // should never happen
    }
}

static constexpr std::array<uint8_t, static_cast<size_t>(messaging_verb::LAST)> make_rpc_client_idx_table() {
    std::array<uint8_t, static_cast<size_t>(messaging_verb::LAST)> tab{};
    for (size_t i = 0; i < tab.size(); ++i) {
        tab[i] = do_get_rpc_client_idx(messaging_verb(i));

        // This SCYLLA_ASSERT guards against adding new connection types without
        // updating *_CONNECTION_COUNT constants.
        SCYLLA_ASSERT(tab[i] < PER_TENANT_CONNECTION_COUNT + PER_SHARD_CONNECTION_COUNT);
    }
    return tab;
}

static std::array<uint8_t, static_cast<size_t>(messaging_verb::LAST)> s_rpc_client_idx_table = make_rpc_client_idx_table();

unsigned
messaging_service::get_rpc_client_idx(messaging_verb verb) const {
    auto idx = s_rpc_client_idx_table[static_cast<size_t>(verb)];

    if (idx < PER_SHARD_CONNECTION_COUNT) {
        return idx;
    }

    // A statement or statement-ack verb
    const auto curr_sched_group = current_scheduling_group();
    for (unsigned i = 0; i < _connection_index_for_tenant.size(); ++i) {
        if (_connection_index_for_tenant[i].sched_group == curr_sched_group) {
            // i == 0: the default tenant maps to the default client indexes belonging to the interval
            // [PER_SHARD_CONNECTION_COUNT, PER_SHARD_CONNECTION_COUNT + PER_TENANT_CONNECTION_COUNT).
            idx += i * PER_TENANT_CONNECTION_COUNT;
            break;
        }
    }
    return idx;
}

std::vector<messaging_service::scheduling_info_for_connection_index>
messaging_service::initial_scheduling_info() const {
    if (_scheduling_config.statement_tenants.empty()) {
        throw std::runtime_error("messaging_service::initial_scheduling_info(): must have at least one tenant configured");
    }
    auto sched_infos = std::vector<scheduling_info_for_connection_index>({
        { _scheduling_config.gossip, "gossip" },
        { _scheduling_config.streaming, "streaming", },
    });

    sched_infos.reserve(sched_infos.size() +
        _scheduling_config.statement_tenants.size() * PER_TENANT_CONNECTION_COUNT);
    for (const auto& tenant : _scheduling_config.statement_tenants) {
        for (auto&& connection_prefix : _connection_types_prefix) {
            sched_infos.push_back({ tenant.sched_group, sstring(connection_prefix) + tenant.name });
        }
    }

    SCYLLA_ASSERT(sched_infos.size() == PER_SHARD_CONNECTION_COUNT +
        _scheduling_config.statement_tenants.size() * PER_TENANT_CONNECTION_COUNT);
    return sched_infos;
};

scheduling_group
messaging_service::scheduling_group_for_verb(messaging_verb verb) const {
    // We are not using get_rpc_client_idx() because it figures out the client
    // index based on the current scheduling group, which is relevant when
    // selecting the right client for sending a message, but is not relevant
    // when registering handlers.
    const auto idx = s_rpc_client_idx_table[static_cast<size_t>(verb)];
    return _scheduling_info_for_connection_index[idx].sched_group;
}

scheduling_group
messaging_service::scheduling_group_for_isolation_cookie(const sstring& isolation_cookie) const {
    // Once per connection, so a loop is fine.
    for (auto&& info : _scheduling_info_for_connection_index) {
        if (info.isolation_cookie == isolation_cookie) {
            return info.sched_group;
        }
    }
    // Check for the case of the client using a connection class we don't
    // recognize, but we know its a tenant, not a system connection.
    // Fall-back to the default tenant in this case.
    for (auto&& connection_prefix : _connection_types_prefix) {
        if (isolation_cookie.find(connection_prefix.data()) == 0) {
            return _scheduling_config.statement_tenants.front().sched_group;
        }
    }
    // Client is using a new connection class that the server doesn't recognize yet.
    // Assume it's important, after server upgrade we'll recognize it.
    return default_scheduling_group();
}


/**
 * Get an IP for a given endpoint to connect to
 *
 * @param ep endpoint to check
 *
 * @return preferred IP (local) for the given endpoint if exists and if the
 *         given endpoint resides in the same data center with the current Node.
 *         Otherwise 'ep' itself is returned.
 */
gms::inet_address messaging_service::get_preferred_ip(gms::inet_address ep) {
    auto it = _preferred_ip_cache.find(ep);

    if (it != _preferred_ip_cache.end()) {
        if (topology_known_for(ep) && is_same_dc(ep)) {
            return it->second;
        }
    }

    // If cache doesn't have an entry for this endpoint - return endpoint itself
    return ep;
}

void messaging_service::init_local_preferred_ip_cache(const std::unordered_map<gms::inet_address, gms::inet_address>& ips_cache) {
    _preferred_ip_cache.clear();
    _preferred_to_endpoint.clear();
    for (auto& p : ips_cache) {
        cache_preferred_ip(p.first, p.second);
    }
}

void messaging_service::cache_preferred_ip(gms::inet_address ep, gms::inet_address ip) {
    if (ip.addr().is_addr_any()) {
        mlogger.warn("Cannot set INADDR_ANY as preferred IP for endpoint {}", ep);
        return;
    }

    _preferred_ip_cache[ep] = ip;
    _preferred_to_endpoint[ip] = ep;
    //
    // Reset the connections to the endpoints that have entries in
    // _preferred_ip_cache so that they reopen with the preferred IPs we've
    // just read.
    //
    remove_rpc_client(msg_addr(ep));
}

gms::inet_address messaging_service::get_public_endpoint_for(const gms::inet_address& ip) const {
    auto i = _preferred_to_endpoint.find(ip);
    return i != _preferred_to_endpoint.end() ? i->second : ip;
}

shared_ptr<messaging_service::rpc_protocol_client_wrapper> messaging_service::get_rpc_client(messaging_verb verb, msg_addr id) {
    SCYLLA_ASSERT(!_shutting_down);
    if (_cfg.maintenance_mode) {
        on_internal_error(mlogger, "This node is in maintenance mode, it shouldn't contact other nodes");
    }
    auto idx = get_rpc_client_idx(verb);
    auto it = _clients[idx].find(id);

    if (it != _clients[idx].end()) {
        auto c = it->second.rpc_client;
        if (!c->error()) {
            return c;
        }
        // The 'dead_only' it should be true, because we're interested in
        // dropping the errored socket, but since it's errored anyway (the
        // above if) it's false to save unneeded second c->error() call
        find_and_remove_client(_clients[idx], id, [] (const auto&) { return true; });
    }

    auto my_host_id = _cfg.id;
    auto broadcast_address = _cfg.broadcast_address;
    bool listen_to_bc = _cfg.listen_on_broadcast_address && _cfg.ip != broadcast_address;
    auto laddr = socket_address(listen_to_bc ? broadcast_address : _cfg.ip, 0);

    std::optional<bool> topology_status;
    auto has_topology = [&] {
        if (!topology_status.has_value()) {
            topology_status = topology_known_for(id.addr) && topology_known_for(broadcast_address);
        }
        return *topology_status;
    };

    auto must_encrypt = [&] {
        if (_cfg.encrypt == encrypt_what::none) {
            return false;
        }

        // See comment above `TOPOLOGY_INDEPENDENT_IDX`.
        if (_cfg.encrypt == encrypt_what::all || idx == TOPOLOGY_INDEPENDENT_IDX) {
            return true;
        }

        // either rack/dc need to be in same dc to use non-tls
        if (!has_topology() || !is_same_dc(id.addr)) {
            return true;
        }

        // #9653 - if our idea of dc for bind address differs from our official endpoint address,
        // we cannot trust downgrading. We need to ensure either (local) bind address is same as
        // broadcast or that the dc info we get for it is the same.
        if (broadcast_address != laddr && !is_same_dc(laddr)) {
            return true;
        }
        // if cross-rack tls, check rack.
        if (_cfg.encrypt == encrypt_what::dc) {
            return false;
        }

        if (!is_same_rack(id.addr)) {
            return true;
        }

        // See above: We need to ensure either (local) bind address is same as
        // broadcast or that the rack info we get for it is the same.
        return broadcast_address != laddr && !is_same_rack(laddr);
    }();

    auto must_compress = [&] {
        if (_cfg.compress == compress_what::none) {
            return false;
        }

        // See comment above `TOPOLOGY_INDEPENDENT_IDX`.
        if (_cfg.compress == compress_what::all || idx == TOPOLOGY_INDEPENDENT_IDX) {
            return true;
        }

        return !has_topology() || !is_same_dc(id.addr);
    }();

    auto must_tcp_nodelay = [&] {
        // See comment above `TOPOLOGY_INDEPENDENT_IDX`.
        if (_cfg.tcp_nodelay == tcp_nodelay_what::all || idx == TOPOLOGY_INDEPENDENT_IDX) {
            return true;
        }

        return !has_topology() || is_same_dc(id.addr);
    }();

    auto addr = get_preferred_ip(id.addr);
    auto remote_addr = socket_address(addr, must_encrypt ? _cfg.ssl_port : _cfg.port);

    rpc::client_options opts;
    // send keepalive messages each minute if connection is idle, drop connection after 10 failures
    opts.keepalive = std::optional<net::tcp_keepalive_params>({60s, 60s, 10});
    if (must_compress) {
        opts.compressor_factory = &compressor_factory;
    }
    opts.tcp_nodelay = must_tcp_nodelay;
    opts.reuseaddr = true;
    opts.isolation_cookie = _scheduling_info_for_connection_index[idx].isolation_cookie;
    opts.metrics_domain = client_metrics_domain(idx, id.addr); // not just `addr` as the latter may be internal IP

    SCYLLA_ASSERT(!must_encrypt || _credentials);

    auto client = must_encrypt ?
                    ::make_shared<rpc_protocol_client_wrapper>(_rpc->protocol(), std::move(opts),
                                    remote_addr, laddr, _credentials) :
                    ::make_shared<rpc_protocol_client_wrapper>(_rpc->protocol(), std::move(opts),
                                    remote_addr, laddr);

    // Remember if we had the peer's topology information when creating the client;
    // if not, we shall later drop the client and create a new one after we learn the peer's
    // topology (so we can use optimal encryption settings and so on for intra-dc/rack messages).
    // But we don't want to apply this logic for TOPOLOGY_INDEPENDENT_IDX client - its settings
    // are independent of topology, so there's no point in dropping it later after we learn
    // the topology (so we always set `topology_ignored` to `false` in that case).
    bool topology_ignored = idx != TOPOLOGY_INDEPENDENT_IDX && topology_status.has_value() && *topology_status == false;
    auto res = _clients[idx].emplace(id, shard_info(std::move(client), topology_ignored));
    SCYLLA_ASSERT(res.second);
    it = res.first;
    uint32_t src_cpu_id = this_shard_id();
    // No reply is received, nothing to wait for.
    (void)_rpc->make_client<
            rpc::no_wait_type(gms::inet_address, uint32_t, uint64_t, utils::UUID)>(messaging_verb::CLIENT_ID)(
                *it->second.rpc_client, broadcast_address, src_cpu_id,
                query::result_memory_limiter::maximum_result_size, my_host_id.uuid())
            .handle_exception([ms = shared_from_this(), remote_addr, verb] (std::exception_ptr ep) {
        mlogger.debug("Failed to send client id to {} for verb {}: {}", remote_addr, std::underlying_type_t<messaging_verb>(verb), ep);
    });
    return it->second.rpc_client;
}

template <typename Fn>
requires std::is_invocable_r_v<bool, Fn, const messaging_service::shard_info&>
void messaging_service::find_and_remove_client(clients_map& clients, msg_addr id, Fn&& filter) {
    if (_shutting_down) {
        // if messaging service is in a processed of been stopped no need to
        // stop and remove connection here since they are being stopped already
        // and we'll just interfere
        return;
    }

    auto it = clients.find(id);
    if (it != clients.end() && filter(it->second)) {
        auto client = std::move(it->second.rpc_client);
        clients.erase(it);
        //
        // Explicitly call rpc_protocol_client_wrapper::stop() for the erased
        // item and hold the messaging_service shared pointer till it's over.
        // This will make sure messaging_service::stop() blocks until
        // client->stop() is over.
        //
        (void)client->stop().finally([id, client, ms = shared_from_this()] {
            mlogger.debug("dropped connection to {}", id.addr);
        }).discard_result();
        _connection_dropped(id.addr);
    }
}

void messaging_service::remove_error_rpc_client(messaging_verb verb, msg_addr id) {
    find_and_remove_client(_clients[get_rpc_client_idx(verb)], id, [] (const auto& s) { return s.rpc_client->error(); });
}

void messaging_service::remove_rpc_client(msg_addr id) {
    for (auto& c : _clients) {
        find_and_remove_client(c, id, [] (const auto&) { return true; });
    }
}

void messaging_service::remove_rpc_client_with_ignored_topology(msg_addr id) {
    for (auto& c : _clients) {
        find_and_remove_client(c, id, [id] (const auto& s) {
            if (s.topology_ignored) {
                mlogger.info("Dropping connection to {} because it was created without topology information", id.addr);
            }
            return s.topology_ignored;
        });
    }
}

std::unique_ptr<messaging_service::rpc_protocol_wrapper>& messaging_service::rpc() {
    return _rpc;
}

rpc::sink<int32_t> messaging_service::make_sink_for_stream_mutation_fragments(rpc::source<frozen_mutation_fragment, rpc::optional<streaming::stream_mutation_fragments_cmd>>& source) {
    return source.make_sink<netw::serializer, int32_t>();
}

future<std::tuple<rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, rpc::source<int32_t>>>
messaging_service::make_sink_and_source_for_stream_mutation_fragments(table_schema_version schema_id, streaming::plan_id plan_id, table_id cf_id, uint64_t estimated_partitions, streaming::stream_reason reason, service::session_id session, msg_addr id) {
    using value_type = std::tuple<rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, rpc::source<int32_t>>;
    if (is_shutting_down()) {
        return make_exception_future<value_type>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(messaging_verb::STREAM_MUTATION_FRAGMENTS, id);
    return rpc_client->make_stream_sink<netw::serializer, frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>().then([this, session, plan_id, schema_id, cf_id, estimated_partitions, reason, rpc_client] (rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd> sink) mutable {
        auto rpc_handler = rpc()->make_client<rpc::source<int32_t> (streaming::plan_id, table_schema_version, table_id, uint64_t, streaming::stream_reason, rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, service::session_id)>(messaging_verb::STREAM_MUTATION_FRAGMENTS);
        return rpc_handler(*rpc_client , plan_id, schema_id, cf_id, estimated_partitions, reason, sink, session).then_wrapped([sink, rpc_client] (future<rpc::source<int32_t>> source) mutable {
            return (source.failed() ? sink.close() : make_ready_future<>()).then([sink = std::move(sink), source = std::move(source)] () mutable {
                return make_ready_future<value_type>(value_type(std::move(sink), source.get()));
            });
        });
    });
}

void messaging_service::register_stream_mutation_fragments(std::function<future<rpc::sink<int32_t>> (const rpc::client_info& cinfo, streaming::plan_id plan_id, table_schema_version schema_id, table_id cf_id, uint64_t estimated_partitions, rpc::optional<streaming::stream_reason>, rpc::source<frozen_mutation_fragment, rpc::optional<streaming::stream_mutation_fragments_cmd>> source, rpc::optional<service::session_id>)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION_FRAGMENTS, std::move(func));
}

future<> messaging_service::unregister_stream_mutation_fragments() {
    return unregister_handler(messaging_verb::STREAM_MUTATION_FRAGMENTS);
}

template<class SinkType, class SourceType>
future<std::tuple<rpc::sink<SinkType>, rpc::source<SourceType>>>
do_make_sink_source(messaging_verb verb, uint32_t repair_meta_id, shard_id dst_shard_id, shared_ptr<messaging_service::rpc_protocol_client_wrapper> rpc_client, std::unique_ptr<messaging_service::rpc_protocol_wrapper>& rpc) {
    using value_type = std::tuple<rpc::sink<SinkType>, rpc::source<SourceType>>;
    auto sink = co_await rpc_client->make_stream_sink<netw::serializer, SinkType>();
    auto rpc_handler = rpc->make_client<rpc::source<SourceType> (uint32_t, rpc::sink<SinkType>, shard_id)>(verb);
    auto source_fut = co_await coroutine::as_future(rpc_handler(*rpc_client, repair_meta_id, sink, dst_shard_id));
    if (source_fut.failed()) {
        auto ex = source_fut.get_exception();
        try {
            co_await sink.close();
        } catch (...) {
            std::throw_with_nested(std::move(ex));
        }
        co_return coroutine::exception(std::move(ex));
    }
    co_return value_type(std::move(sink), std::move(source_fut.get()));
}

// Wrapper for REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM
future<std::tuple<rpc::sink<repair_hash_with_cmd>, rpc::source<repair_row_on_wire_with_cmd>>>
messaging_service::make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, msg_addr id) {
    auto verb = messaging_verb::REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM;
    if (is_shutting_down()) {
        return make_exception_future<std::tuple<rpc::sink<repair_hash_with_cmd>, rpc::source<repair_row_on_wire_with_cmd>>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, id);
    return do_make_sink_source<repair_hash_with_cmd, repair_row_on_wire_with_cmd>(verb, repair_meta_id, dst_cpu_id, std::move(rpc_client), rpc());
}

rpc::sink<repair_row_on_wire_with_cmd> messaging_service::make_sink_for_repair_get_row_diff_with_rpc_stream(rpc::source<repair_hash_with_cmd>& source) {
    return source.make_sink<netw::serializer, repair_row_on_wire_with_cmd>();
}

void messaging_service::register_repair_get_row_diff_with_rpc_stream(std::function<future<rpc::sink<repair_row_on_wire_with_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_hash_with_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM, std::move(func));
}
future<> messaging_service::unregister_repair_get_row_diff_with_rpc_stream() {
    return unregister_handler(messaging_verb::REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM);
}

// Wrapper for REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM
future<std::tuple<rpc::sink<repair_row_on_wire_with_cmd>, rpc::source<repair_stream_cmd>>>
messaging_service::make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, msg_addr id) {
    auto verb = messaging_verb::REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM;
    if (is_shutting_down()) {
        return make_exception_future<std::tuple<rpc::sink<repair_row_on_wire_with_cmd>, rpc::source<repair_stream_cmd>>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, id);
    return do_make_sink_source<repair_row_on_wire_with_cmd, repair_stream_cmd>(verb, repair_meta_id, dst_cpu_id, std::move(rpc_client), rpc());
}

rpc::sink<repair_stream_cmd> messaging_service::make_sink_for_repair_put_row_diff_with_rpc_stream(rpc::source<repair_row_on_wire_with_cmd>& source) {
    return source.make_sink<netw::serializer, repair_stream_cmd>();
}

void messaging_service::register_repair_put_row_diff_with_rpc_stream(std::function<future<rpc::sink<repair_stream_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_row_on_wire_with_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt)>&& func) {
    register_handler(this, messaging_verb::REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM, std::move(func));
}
future<> messaging_service::unregister_repair_put_row_diff_with_rpc_stream() {
    return unregister_handler(messaging_verb::REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM);
}

// Wrapper for REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM
future<std::tuple<rpc::sink<repair_stream_cmd>, rpc::source<repair_hash_with_cmd>>>
messaging_service::make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, msg_addr id) {
    auto verb = messaging_verb::REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM;
    if (is_shutting_down()) {
        return make_exception_future<std::tuple<rpc::sink<repair_stream_cmd>, rpc::source<repair_hash_with_cmd>>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, id);
    return do_make_sink_source<repair_stream_cmd, repair_hash_with_cmd>(verb, repair_meta_id, dst_cpu_id, std::move(rpc_client), rpc());
}

rpc::sink<repair_hash_with_cmd> messaging_service::make_sink_for_repair_get_full_row_hashes_with_rpc_stream(rpc::source<repair_stream_cmd>& source) {
    return source.make_sink<netw::serializer, repair_hash_with_cmd>();
}

void messaging_service::register_repair_get_full_row_hashes_with_rpc_stream(std::function<future<rpc::sink<repair_hash_with_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_stream_cmd> source, rpc::optional<shard_id> dst_cpu_id_opt)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM, std::move(func));
}
future<> messaging_service::unregister_repair_get_full_row_hashes_with_rpc_stream() {
    return unregister_handler(messaging_verb::REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM);
}

// Wrappers for verbs

// PREPARE_MESSAGE
void messaging_service::register_prepare_message(std::function<future<streaming::prepare_message> (const rpc::client_info& cinfo,
        streaming::prepare_message msg, streaming::plan_id plan_id, sstring description, rpc::optional<streaming::stream_reason> reason, rpc::optional<service::session_id>)>&& func) {
    register_handler(this, messaging_verb::PREPARE_MESSAGE, std::move(func));
}
future<streaming::prepare_message> messaging_service::send_prepare_message(msg_addr id, streaming::prepare_message msg, streaming::plan_id plan_id,
        sstring description, streaming::stream_reason reason, service::session_id session) {
    return send_message<streaming::prepare_message>(this, messaging_verb::PREPARE_MESSAGE, id,
        std::move(msg), plan_id, std::move(description), reason, session);
}
future<> messaging_service::unregister_prepare_message() {
    return unregister_handler(messaging_verb::PREPARE_MESSAGE);
}

// PREPARE_DONE_MESSAGE
void messaging_service::register_prepare_done_message(std::function<future<> (const rpc::client_info& cinfo, streaming::plan_id plan_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::PREPARE_DONE_MESSAGE, std::move(func));
}
future<> messaging_service::send_prepare_done_message(msg_addr id, streaming::plan_id plan_id, unsigned dst_cpu_id) {
    return send_message<void>(this, messaging_verb::PREPARE_DONE_MESSAGE, id,
        plan_id, dst_cpu_id);
}
future<> messaging_service::unregister_prepare_done_message() {
    return unregister_handler(messaging_verb::PREPARE_DONE_MESSAGE);
}

// STREAM_MUTATION_DONE
void messaging_service::register_stream_mutation_done(std::function<future<> (const rpc::client_info& cinfo,
        streaming::plan_id plan_id, dht::token_range_vector ranges, table_id cf_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION_DONE,
            [func = std::move(func)] (const rpc::client_info& cinfo,
                    streaming::plan_id plan_id, std::vector<wrapping_interval<dht::token>> ranges,
                    table_id cf_id, unsigned dst_cpu_id) mutable {
        return func(cinfo, plan_id, ::compat::unwrap(std::move(ranges)), cf_id, dst_cpu_id);
    });
}
future<> messaging_service::send_stream_mutation_done(msg_addr id, streaming::plan_id plan_id, dht::token_range_vector ranges, table_id cf_id, unsigned dst_cpu_id) {
    return send_message<void>(this, messaging_verb::STREAM_MUTATION_DONE, id,
        plan_id, std::move(ranges), cf_id, dst_cpu_id);
}
future<> messaging_service::unregister_stream_mutation_done() {
    return unregister_handler(messaging_verb::STREAM_MUTATION_DONE);
}

// COMPLETE_MESSAGE
void messaging_service::register_complete_message(std::function<future<> (const rpc::client_info& cinfo, streaming::plan_id plan_id, unsigned dst_cpu_id, rpc::optional<bool> failed)>&& func) {
    register_handler(this, messaging_verb::COMPLETE_MESSAGE, std::move(func));
}
future<> messaging_service::send_complete_message(msg_addr id, streaming::plan_id plan_id, unsigned dst_cpu_id, bool failed) {
    return send_message<void>(this, messaging_verb::COMPLETE_MESSAGE, id,
        plan_id, dst_cpu_id, failed);
}
future<> messaging_service::unregister_complete_message() {
    return unregister_handler(messaging_verb::COMPLETE_MESSAGE);
}

void messaging_service::register_definitions_update(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, std::vector<frozen_mutation> fm,
            rpc::optional<std::vector<canonical_mutation>> cm)>&& func) {
    register_handler(this, netw::messaging_verb::DEFINITIONS_UPDATE, std::move(func));
}
future<> messaging_service::unregister_definitions_update() {
    return unregister_handler(netw::messaging_verb::DEFINITIONS_UPDATE);
}
future<> messaging_service::send_definitions_update(msg_addr id, std::vector<frozen_mutation> fm, std::vector<canonical_mutation> cm) {
    return send_message_oneway(this, messaging_verb::DEFINITIONS_UPDATE, std::move(id), std::move(fm), std::move(cm));
}

void messaging_service::register_migration_request(std::function<future<rpc::tuple<std::vector<frozen_mutation>, std::vector<canonical_mutation>>>
        (const rpc::client_info&, rpc::optional<schema_pull_options>)>&& func) {
    register_handler(this, netw::messaging_verb::MIGRATION_REQUEST, std::move(func));
}
future<> messaging_service::unregister_migration_request() {
    return unregister_handler(netw::messaging_verb::MIGRATION_REQUEST);
}
future<rpc::tuple<std::vector<frozen_mutation>, rpc::optional<std::vector<canonical_mutation>>>> messaging_service::send_migration_request(msg_addr id,
        schema_pull_options options) {
    return send_message<future<rpc::tuple<std::vector<frozen_mutation>, rpc::optional<std::vector<canonical_mutation>>>>>(this, messaging_verb::MIGRATION_REQUEST,
            std::move(id), options);
}
future<rpc::tuple<std::vector<frozen_mutation>, rpc::optional<std::vector<canonical_mutation>>>> messaging_service::send_migration_request(msg_addr id,
        abort_source& as, schema_pull_options options) {
    return send_message_cancellable<future<rpc::tuple<std::vector<frozen_mutation>, rpc::optional<std::vector<canonical_mutation>>>>>(this, messaging_verb::MIGRATION_REQUEST,
            std::move(id), as, options);
}

void messaging_service::register_get_schema_version(std::function<future<frozen_schema>(unsigned, table_schema_version)>&& func) {
    register_handler(this, netw::messaging_verb::GET_SCHEMA_VERSION, std::move(func));
}
future<> messaging_service::unregister_get_schema_version() {
    return unregister_handler(netw::messaging_verb::GET_SCHEMA_VERSION);
}
future<frozen_schema> messaging_service::send_get_schema_version(msg_addr dst, table_schema_version v) {
    return send_message<frozen_schema>(this, messaging_verb::GET_SCHEMA_VERSION, dst, static_cast<unsigned>(dst.cpu_id), v);
}

void messaging_service::register_schema_check(std::function<future<table_schema_version>()>&& func) {
    register_handler(this, netw::messaging_verb::SCHEMA_CHECK, std::move(func));
}
future<> messaging_service::unregister_schema_check() {
    return unregister_handler(netw::messaging_verb::SCHEMA_CHECK);
}
future<table_schema_version> messaging_service::send_schema_check(msg_addr dst) {
    return send_message<table_schema_version>(this, netw::messaging_verb::SCHEMA_CHECK, dst);
}
future<table_schema_version> messaging_service::send_schema_check(msg_addr dst, abort_source& as) {
    return send_message_cancellable<table_schema_version>(this, netw::messaging_verb::SCHEMA_CHECK, dst, as);
}

// Wrapper for REPAIR_GET_FULL_ROW_HASHES
void messaging_service::register_repair_get_full_row_hashes(std::function<future<repair_hash_set> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_FULL_ROW_HASHES, std::move(func));
}
future<> messaging_service::unregister_repair_get_full_row_hashes() {
    return unregister_handler(messaging_verb::REPAIR_GET_FULL_ROW_HASHES);
}
future<repair_hash_set> messaging_service::send_repair_get_full_row_hashes(msg_addr id, uint32_t repair_meta_id, shard_id dst_shard_id) {
    return send_message<future<repair_hash_set>>(this, messaging_verb::REPAIR_GET_FULL_ROW_HASHES, std::move(id), repair_meta_id, dst_shard_id);
}

// Wrapper for REPAIR_GET_COMBINED_ROW_HASH
void messaging_service::register_repair_get_combined_row_hash(std::function<future<get_combined_row_hash_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_COMBINED_ROW_HASH, std::move(func));
}
future<> messaging_service::unregister_repair_get_combined_row_hash() {
    return unregister_handler(messaging_verb::REPAIR_GET_COMBINED_ROW_HASH);
}
future<get_combined_row_hash_response> messaging_service::send_repair_get_combined_row_hash(msg_addr id, uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary, shard_id dst_shard_id) {
    return send_message<future<get_combined_row_hash_response>>(this, messaging_verb::REPAIR_GET_COMBINED_ROW_HASH, std::move(id), repair_meta_id, std::move(common_sync_boundary), dst_shard_id);
}

void messaging_service::register_repair_get_sync_boundary(std::function<future<get_sync_boundary_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_SYNC_BOUNDARY, std::move(func));
}
future<> messaging_service::unregister_repair_get_sync_boundary() {
    return unregister_handler(messaging_verb::REPAIR_GET_SYNC_BOUNDARY);
}
future<get_sync_boundary_response> messaging_service::send_repair_get_sync_boundary(msg_addr id, uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary, shard_id dst_shard_id) {
    return send_message<future<get_sync_boundary_response>>(this, messaging_verb::REPAIR_GET_SYNC_BOUNDARY, std::move(id), repair_meta_id, std::move(skipped_sync_boundary), dst_shard_id);
}

// Wrapper for REPAIR_GET_ROW_DIFF
void messaging_service::register_repair_get_row_diff(std::function<future<repair_rows_on_wire> (const rpc::client_info& cinfo, uint32_t repair_meta_id, repair_hash_set set_diff, bool needs_all_rows, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_ROW_DIFF, std::move(func));
}
future<> messaging_service::unregister_repair_get_row_diff() {
    return unregister_handler(messaging_verb::REPAIR_GET_ROW_DIFF);
}
future<repair_rows_on_wire> messaging_service::send_repair_get_row_diff(msg_addr id, uint32_t repair_meta_id, repair_hash_set set_diff, bool needs_all_rows, shard_id dst_shard_id) {
    return send_message<future<repair_rows_on_wire>>(this, messaging_verb::REPAIR_GET_ROW_DIFF, std::move(id), repair_meta_id, std::move(set_diff), needs_all_rows, dst_shard_id);
}

// Wrapper for REPAIR_PUT_ROW_DIFF
void messaging_service::register_repair_put_row_diff(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, repair_rows_on_wire row_diff, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_PUT_ROW_DIFF, std::move(func));
}
future<> messaging_service::unregister_repair_put_row_diff() {
    return unregister_handler(messaging_verb::REPAIR_PUT_ROW_DIFF);
}
future<> messaging_service::send_repair_put_row_diff(msg_addr id, uint32_t repair_meta_id, repair_rows_on_wire row_diff, shard_id dst_shard_id) {
    return send_message<void>(this, messaging_verb::REPAIR_PUT_ROW_DIFF, std::move(id), repair_meta_id, std::move(row_diff), dst_shard_id);
}

// Wrapper for REPAIR_ROW_LEVEL_START
void messaging_service::register_repair_row_level_start(std::function<future<repair_row_level_start_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, rpc::optional<streaming::stream_reason> reason, rpc::optional<gc_clock::time_point> compaction_time, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_ROW_LEVEL_START, std::move(func));
}
future<> messaging_service::unregister_repair_row_level_start() {
    return unregister_handler(messaging_verb::REPAIR_ROW_LEVEL_START);
}
future<rpc::optional<repair_row_level_start_response>> messaging_service::send_repair_row_level_start(msg_addr id, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, streaming::stream_reason reason, gc_clock::time_point compaction_time, shard_id dst_shard_id) {
    return send_message<rpc::optional<repair_row_level_start_response>>(this, messaging_verb::REPAIR_ROW_LEVEL_START, std::move(id), repair_meta_id, std::move(keyspace_name), std::move(cf_name), std::move(range), algo, max_row_buf_size, seed, remote_shard, remote_shard_count, remote_ignore_msb, std::move(remote_partitioner_name), std::move(schema_version), reason, compaction_time, dst_shard_id);
}

// Wrapper for REPAIR_ROW_LEVEL_STOP
void messaging_service::register_repair_row_level_stop(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_ROW_LEVEL_STOP, std::move(func));
}
future<> messaging_service::unregister_repair_row_level_stop() {
    return unregister_handler(messaging_verb::REPAIR_ROW_LEVEL_STOP);
}
future<> messaging_service::send_repair_row_level_stop(msg_addr id, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, shard_id dst_shard_id) {
    return send_message<void>(this, messaging_verb::REPAIR_ROW_LEVEL_STOP, std::move(id), repair_meta_id, std::move(keyspace_name), std::move(cf_name), std::move(range), dst_shard_id);
}

// Wrapper for REPAIR_GET_ESTIMATED_PARTITIONS
void messaging_service::register_repair_get_estimated_partitions(std::function<future<uint64_t> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_ESTIMATED_PARTITIONS, std::move(func));
}
future<> messaging_service::unregister_repair_get_estimated_partitions() {
    return unregister_handler(messaging_verb::REPAIR_GET_ESTIMATED_PARTITIONS);
}
future<uint64_t> messaging_service::send_repair_get_estimated_partitions(msg_addr id, uint32_t repair_meta_id, shard_id dst_shard_id) {
    return send_message<future<uint64_t>>(this, messaging_verb::REPAIR_GET_ESTIMATED_PARTITIONS, std::move(id), repair_meta_id, dst_shard_id);
}

// Wrapper for REPAIR_SET_ESTIMATED_PARTITIONS
void messaging_service::register_repair_set_estimated_partitions(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, uint64_t estimated_partitions, rpc::optional<shard_id> dst_shard_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_SET_ESTIMATED_PARTITIONS, std::move(func));
}
future<> messaging_service::unregister_repair_set_estimated_partitions() {
    return unregister_handler(messaging_verb::REPAIR_SET_ESTIMATED_PARTITIONS);
}
future<> messaging_service::send_repair_set_estimated_partitions(msg_addr id, uint32_t repair_meta_id, uint64_t estimated_partitions, shard_id dst_shard_id) {
    return send_message<void>(this, messaging_verb::REPAIR_SET_ESTIMATED_PARTITIONS, std::move(id), repair_meta_id, estimated_partitions, dst_shard_id);
}

// Wrapper for REPAIR_GET_DIFF_ALGORITHMS
void messaging_service::register_repair_get_diff_algorithms(std::function<future<std::vector<row_level_diff_detect_algorithm>> (const rpc::client_info& cinfo)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_DIFF_ALGORITHMS, std::move(func));
}
future<> messaging_service::unregister_repair_get_diff_algorithms() {
    return unregister_handler(messaging_verb::REPAIR_GET_DIFF_ALGORITHMS);
}
future<std::vector<row_level_diff_detect_algorithm>> messaging_service::send_repair_get_diff_algorithms(msg_addr id) {
    return send_message<future<std::vector<row_level_diff_detect_algorithm>>>(this, messaging_verb::REPAIR_GET_DIFF_ALGORITHMS, std::move(id));
}

// Wrapper for NODE_OPS_CMD
void messaging_service::register_node_ops_cmd(std::function<future<node_ops_cmd_response> (const rpc::client_info& cinfo, node_ops_cmd_request)>&& func) {
    register_handler(this, messaging_verb::NODE_OPS_CMD, std::move(func));
}
future<> messaging_service::unregister_node_ops_cmd() {
    return unregister_handler(messaging_verb::NODE_OPS_CMD);
}
future<node_ops_cmd_response> messaging_service::send_node_ops_cmd(msg_addr id, node_ops_cmd_request req) {
    return send_message<future<node_ops_cmd_response>>(this, messaging_verb::NODE_OPS_CMD, std::move(id), std::move(req));
}

// Wrapper for TASKS_CHILDREN_REQUEST
void messaging_service::register_tasks_get_children(std::function<future<tasks::get_children_response> (const rpc::client_info& cinfo, tasks::get_children_request)>&& func) {
    register_handler(this, messaging_verb::TASKS_GET_CHILDREN, std::move(func));
}
future<> messaging_service::unregister_tasks_get_children() {
    return unregister_handler(messaging_verb::TASKS_GET_CHILDREN);
}
future<tasks::get_children_response> messaging_service::send_tasks_get_children(msg_addr id, tasks::get_children_request req) {
    return send_message<future<tasks::get_children_response>>(this, messaging_verb::TASKS_GET_CHILDREN, std::move(id), std::move(req));
}

} // namespace net
