/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "gms/inet_address.hh"
#include "utils/assert.hh"
#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/all.hh>

#include "message/messaging_service.hh"
#include <seastar/core/distributed.hh>
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "service/qos/service_level_controller.hh"
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
#include "service/tablet_operation.hh"
#include "service/topology_state_machine.hh"
#include "service/topology_guard.hh"
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
#include "idl/repair.dist.hh"
#include "idl/node_ops.dist.hh"
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
#include "idl/migration_manager.dist.hh"
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
#include "idl/migration_manager.dist.impl.hh"
#include <seastar/rpc/lz4_compressor.hh>
#include <seastar/rpc/lz4_fragmented_compressor.hh>
#include <seastar/rpc/multi_algo_compressor_factory.hh>
#include "partition_range_compat.hh"
#include "mutation/frozen_mutation.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "idl/repair.dist.impl.hh"
#include "idl/node_ops.dist.impl.hh"
#include "idl/mapreduce_request.dist.hh"
#include "idl/mapreduce_request.dist.impl.hh"
#include "idl/storage_service.dist.impl.hh"
#include "idl/join_node.dist.impl.hh"
#include "gms/feature_service.hh"

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

class messaging_service::compressor_factory_wrapper {
    struct advanced_rpc_compressor_factory : rpc::compressor::factory {
        utils::walltime_compressor_tracker& _tracker;
        advanced_rpc_compressor_factory(utils::walltime_compressor_tracker& tracker)
            : _tracker(tracker)
        {}
        const sstring& supported() const override {
            return _tracker.supported();
        }
        std::unique_ptr<rpc::compressor> negotiate(sstring feature, bool is_server, std::function<future<>()> send_empty_frame) const override {
            return _tracker.negotiate(std::move(feature), is_server, std::move(send_empty_frame));
        }
        std::unique_ptr<rpc::compressor> negotiate(sstring feature, bool is_server) const override {
            assert(false && "negotiate() without send_empty_frame shouldn't happen");
            return nullptr;
        }
    };
    rpc::lz4_fragmented_compressor::factory _lz4_fragmented_compressor_factory;
    rpc::lz4_compressor::factory _lz4_compressor_factory;
    advanced_rpc_compressor_factory _arcf;
    rpc::multi_algo_compressor_factory _multi_factory;
public:
    compressor_factory_wrapper(decltype(advanced_rpc_compressor_factory::_tracker) t, bool enable_advanced)
        : _arcf(t)
        , _multi_factory(enable_advanced ? rpc::multi_algo_compressor_factory{
            &_arcf,
            &_lz4_fragmented_compressor_factory,
            &_lz4_compressor_factory,
        } : rpc::multi_algo_compressor_factory{
            &_lz4_fragmented_compressor_factory,
            &_lz4_compressor_factory,
        })
    {}
    seastar::rpc::compressor::factory& get_factory() {
        return _multi_factory;
    }
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

messaging_service::shard_info::shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client, bool topo_ignored, inet_address ip)
    : rpc_client(std::move(client))
    , topology_ignored(topo_ignored), endpoint(ip)
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

future<> messaging_service::unregister_handler(messaging_verb verb) {
    return _rpc->unregister_handler(verb);
}

messaging_service::messaging_service(
    locator::host_id id,
    gms::inet_address ip,
    uint16_t port,
    gms::feature_service& feature_service,
    gms::gossip_address_map& address_map,
    utils::walltime_compressor_tracker& wct,
    qos::service_level_controller& sl_controller)
    : messaging_service(config{std::move(id), ip, ip, port},
                        scheduling_config{{{{}, "$default"}}, {}, {}},
                        nullptr, feature_service, address_map, wct, sl_controller)
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

future<> messaging_service::start_listen(locator::shared_token_metadata& stm, std::function<locator::host_id(gms::inet_address)> address_to_host_id_mapper) {
    _token_metadata = &stm;
    _address_to_host_id_mapper = std::move(address_to_host_id_mapper);
    do_start_listen();
    return make_ready_future<>();
}

bool messaging_service::topology_known_for(inet_address addr) const {
    // The token metadata pointer is nullptr before
    // the service is start_listen()-ed and after it's being shutdown()-ed.
    const locator::node* node;
    return _token_metadata
        && (node = _token_metadata->get()->get_topology().find_node(_address_to_host_id_mapper(addr))) && !node->is_none();
}

// Precondition: `topology_known_for(addr)`.
bool messaging_service::is_same_dc(inet_address addr) const {
    const auto& topo = _token_metadata->get()->get_topology();
    return topo.get_datacenter(_address_to_host_id_mapper(addr)) == topo.get_datacenter();
}

// Precondition: `topology_known_for(addr)`.
bool messaging_service::is_same_rack(inet_address addr) const {
    const auto& topo = _token_metadata->get()->get_topology();
    return topo.get_rack(_address_to_host_id_mapper(addr)) == topo.get_rack();
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
sstring messaging_service::client_metrics_domain(unsigned idx, inet_address addr, std::optional<locator::host_id> id) const {
    sstring ret = _scheduling_info_for_connection_index[idx].isolation_cookie;
    if (!id) {
        id = _address_to_host_id_mapper(addr);
    }
    if (_token_metadata) {
        const auto& topo = _token_metadata->get()->get_topology();
        if (topo.has_node(*id)) {
            ret += ":" + topo.get_datacenter(*id);
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
        so.compressor_factory = &_compressor_factory_wrapper->get_factory();
    }
    so.load_balancing_algorithm = server_socket::load_balancing_algorithm::port;

    // FIXME: we don't set so.tcp_nodelay, because we can't tell at this point whether the connection will come from a
    //        local or remote datacenter, and whether or not the connection will be used for gossip. We can fix
    //        the first by wrapping its server_socket, but not the second.
    auto limits = rpc_resource_limits(_cfg.rpc_memory_limit);
    limits.isolate_connection = [this] (sstring isolation_cookie) {

        return scheduling_group_for_isolation_cookie(isolation_cookie).then([] (scheduling_group sg) {
            return rpc::isolation_config{.sched_group = sg};
        });
    };
    if (!_server[0] && _cfg.encrypt != encrypt_what::all && _cfg.port) {
        auto listen = [&] (const gms::inet_address& a, rpc::streaming_domain_type sdomain) {
            so.streaming_domain = sdomain;
            so.filter_connection = {};
            switch (_cfg.encrypt) {
                default:
                case encrypt_what::none:
                case encrypt_what::transitional:
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
                // TODO: the condition to skip this if cfg.port == 0 is mainly to appease dtest.
                // remove once we've adjusted those tests.
                if (_cfg.encrypt == encrypt_what::none && (!_credentials || _cfg.port == 0)) {
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

messaging_service::messaging_service(config cfg, scheduling_config scfg, std::shared_ptr<seastar::tls::credentials_builder> credentials, gms::feature_service& feature_service,
                                     gms::gossip_address_map& address_map, utils::walltime_compressor_tracker& arct, qos::service_level_controller& sl_controller)
    : _cfg(std::move(cfg))
    , _rpc(new rpc_protocol_wrapper(serializer { }))
    , _credentials_builder(credentials ? std::make_unique<seastar::tls::credentials_builder>(*credentials) : nullptr)
    , _clients(PER_SHARD_CONNECTION_COUNT + scfg.statement_tenants.size() * PER_TENANT_CONNECTION_COUNT)
    , _clients_with_host_id(PER_SHARD_CONNECTION_COUNT + scfg.statement_tenants.size() * PER_TENANT_CONNECTION_COUNT)
    , _scheduling_config(scfg)
    , _scheduling_info_for_connection_index(initial_scheduling_info())
    , _feature_service(feature_service)
    , _sl_controller(sl_controller)
    , _compressor_factory_wrapper(std::make_unique<compressor_factory_wrapper>(arct, _cfg.enable_advanced_rpc_compression))
    , _address_map(address_map)
{
    _rpc->set_logger(&rpc_logger);

    // this initialization should be done before any handler registration
    // this is because register_handler calls to: scheduling_group_for_verb
    // which in turn relies on _connection_index_for_tenant to be initialized.
    _connection_index_for_tenant.reserve(_scheduling_config.statement_tenants.size());
    for (unsigned i = 0; i <  _scheduling_config.statement_tenants.size(); ++i) {
        auto& tenant_cfg = _scheduling_config.statement_tenants[i];
        _connection_index_for_tenant.push_back({tenant_cfg.sched_group, i, tenant_cfg.enabled});
    }

    register_handler(this, messaging_verb::CLIENT_ID, [this] (rpc::client_info& ci, gms::inet_address broadcast_address, uint32_t src_cpu_id, rpc::optional<uint64_t> max_result_size, rpc::optional<utils::UUID> host_id,
                    rpc::optional<std::optional<utils::UUID>> dst_host_id) {
        if (dst_host_id && *dst_host_id && **dst_host_id != _cfg.id.uuid()) {
            ci.server.abort_connection(ci.conn_id);
        }
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
    init_feature_listeners();
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
            servers | std::views::filter([] (auto& ptr) { return bool(ptr); }) | std::views::transform([] (auto& ptr) -> messaging_service::rpc_protocol_server_wrapper& { return *ptr; }),
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
    auto d = defer([] { mlogger.info("Stopped clients"); });
    auto stop_clients = [] (auto& clients) ->future<> {
        co_await coroutine::parallel_for_each(clients, [] (auto& m) -> future<> {
            auto d = defer([&m] {
                // no new clients should be added by get_rpc_client(), as it
                // asserts that _shutting_down is true
                m.clear();
            });
            co_await coroutine::parallel_for_each(m, [] (auto& c) -> future<> {
                mlogger.info("Stopping client for address: {}", c.first);
                co_await c.second.rpc_client->stop();
                mlogger.info("Stopping client for address: {} - Done", c.first);
            });
        });
    };
    co_await coroutine::all(
        [&] { return stop_clients(_clients); },
        [&] { return stop_clients(_clients_with_host_id); }
    );
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
    case messaging_verb::TABLET_REPAIR:
    case messaging_verb::TABLE_LOAD_STATS:
        return 1;
    case messaging_verb::CLIENT_ID:
    case messaging_verb::MUTATION:
    case messaging_verb::READ_DATA:
    case messaging_verb::READ_MUTATION_DATA:
    case messaging_verb::READ_DIGEST:
    case messaging_verb::DEFINITIONS_UPDATE:
    case messaging_verb::TRUNCATE:
    case messaging_verb::TRUNCATE_WITH_TABLETS:
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

msg_addr messaging_service::addr_for_host_id(locator::host_id hid) {
    auto opt_ip = _address_map.find(hid);
    if (!opt_ip) {
        throw unknown_address(hid);
    }
    return msg_addr{*opt_ip, 0};
}

unsigned
messaging_service::get_rpc_client_idx(messaging_verb verb) {
    auto idx = s_rpc_client_idx_table[static_cast<size_t>(verb)];

    if (idx < PER_SHARD_CONNECTION_COUNT) {
        return idx;
    }

    // this is just a workaround for a wrong initialization order in messaging_service's
    // constructor that causes _connection_index_for_tenant to be queried before it is
    // initialized. This WA makes the behaviour match OSS in this case and it should be
    // removed once it is fixed in OSS. If it isn't removed the behaviour will still be
    // correct but we will lose cycles on an unnecesairy check.
    if (_connection_index_for_tenant.size() == 0) {
        return idx;
    }
    const auto curr_sched_group = current_scheduling_group();
    for (unsigned i = 0; i < _connection_index_for_tenant.size(); ++i) {
        if (_connection_index_for_tenant[i].sched_group == curr_sched_group) {
            if (_connection_index_for_tenant[i].enabled) {
                // i == 0: the default tenant maps to the default client indexes belonging to the interval
                // [PER_SHARD_CONNECTION_COUNT, PER_SHARD_CONNECTION_COUNT + PER_TENANT_CONNECTION_COUNT).
                idx += i * PER_TENANT_CONNECTION_COUNT;
                return idx;
            } else {
                // If the tenant is disable, immediately return current index to
                // use $system tenant.
                return idx;
            }
        }

    }

    // if we got here - it means that two conditions are met:
    // 1. We are trying to get a client for a statement/statement_ack verb.
    // 2. We are running in a scheduling group that is not assigned to one of the
    // static tenants (e.g $system)
    // If this scheduling group is of one of the system's static statement tenants we
    // would have caught it in the loop above.
    // The other possibility is that we are running in a scheduling group belongs to
    // a service level, maybe a deleted one, this is why it is possible that we will
    // not find the service level name.

    std::optional<sstring> service_level = _sl_controller.get_active_service_level();
    scheduling_group sg_for_tenant = curr_sched_group;
    if (!service_level) {
        service_level = qos::service_level_controller::default_service_level_name;
        sg_for_tenant = _sl_controller.get_default_scheduling_group();
    }
    auto it = _dynamic_tenants_to_client_idx.find(*service_level);
    // the second part of this condition checks that the service level didn't "suddenly"
    // changed scheduling group. If it did, it means probably that it was dropped and
    // added again, if it happens we will update it's connection indexes since it is
    // basically a new tenant with the same name.
    if (it == _dynamic_tenants_to_client_idx.end() ||
            _scheduling_info_for_connection_index[it->second].sched_group != sg_for_tenant) {
        return add_statement_tenant(*service_level,sg_for_tenant) + (idx - PER_SHARD_CONNECTION_COUNT);
    }
    return it->second;
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

future<scheduling_group>
messaging_service::scheduling_group_for_isolation_cookie(const sstring& isolation_cookie) const {
    // Once per connection, so a loop is fine.
    for (auto&& info : _scheduling_info_for_connection_index) {
        if (info.isolation_cookie == isolation_cookie) {
            return make_ready_future<scheduling_group>(info.sched_group);
        }
    }

    // We first check if this is a statement isolation cookie - if it is, we will search for the
    // appropriate service level in the service_level_controller since in can be that
    // _scheduling_info_for_connection_index is not yet updated (drop read case for example)
    // in the future we will only fall back here for new service levels that haven't been referenced
    // before.
    // It is safe to assume that an unknown connection type can be rejected since a connection
    // with an unknown purpose on the inbound side is useless.
    // However, until we get rid of the backward compatibility code below, we can't reject the
    // connection since there is a slight chance that this connection comes from an old node that
    // still doesn't use the "connection type prefix" convention.
    auto tenant_connection = [] (const sstring& isolation_cookie) -> bool {
        for (auto&& connection_prefix : _connection_types_prefix) {
            if(isolation_cookie.find(connection_prefix.data()) == 0) {
                return true;
            }
        }
        return false;
    };

    std::string service_level_name = "";
    if (tenant_connection(isolation_cookie)) {
        // Extract the service level name from the connection isolation cookie.
        service_level_name = isolation_cookie.substr(std::string(isolation_cookie).find_first_of(':') + 1);
    } else if (_sl_controller.has_service_level(isolation_cookie)) {
        // Backward Compatibility Code - This entire "else if" block should be removed
        // in the major version that follows the one that contains this code.
        // When upgrading from an older enterprise version the isolation cookie is not
        // prefixed with "statement:" or any other connection type prefix, so an isolation cookie
        // that comes from an older node will simply contain the service level name.
        // we do an extra step to be also future proof and make sure it is indeed a service
        // level's name, since if this is the older version and we upgrade to a new one
        // we could have more connection classes (eg: streaming,gossip etc...) and we wouldn't
        // want it to overload the default statement's scheduling group.
        // it is not bulet proof in the sense that if a new tenant class happens to have the exact
        // name as one of the service levels it will be diverted to the default statement scheduling
        // group but it has a small chance of happening.
        service_level_name = isolation_cookie;
        mlogger.info("Trying to allow an rpc connection from an older node for service level {}", service_level_name);
    } else {
        // Client is using a new connection class that the server doesn't recognize yet.
        // Assume it's important, after server upgrade we'll recognize it.
        service_level_name = isolation_cookie;
        mlogger.warn("Assuming an unknown cookie is from an older node and represent some not yet discovered service level {} - Trying to allow it.", service_level_name);
    }

    if (_sl_controller.has_service_level(service_level_name)) {
        return make_ready_future<scheduling_group>(_sl_controller.get_scheduling_group(service_level_name));
    } else if (service_level_name.starts_with('$')) {
        // Tenant names starting with '$' are reserved for internal ones. If the tenant is not recognized
        // to this point, it means we are in the middle of cluster upgrade and we don't know this tenant yet.
        // Hardwire it to the default service level to keep things simple.
        // This also includes `$user` tenant which is used in OSS and may appear in mixed OSS/Enterprise cluster.
        return make_ready_future<scheduling_group>(_sl_controller.get_default_scheduling_group());
    } else {
        mlogger.info("Service level {} is still unknown, will try to create it now and allow the RPC connection.", service_level_name);
        // If the service level don't exist there are two possibilities, it is either created but still not known by this
        // node. Or it has been deleted and the initiating node hasn't caught up yet, in both cases it is safe to __try__ and
        // create a new service level (internally), it will naturally catch up eventually and by creating it here we prevent
        // an rpc connection for a valid service level to permanently get stuck in the default service level scheduling group.
        // If we can't create the service level (we already have too many service levels), we will reject the connection by returning
        // an exceptional future.
        qos::service_level_options slo;
        // We put here the minimal amount of shares for this service level to be functional. When the node catches up it will
        // be either deleted or the number of shares and other configuration options will be updated.
        slo.shares.emplace<int32_t>(1000);
        slo.shares_name.emplace(service_level_name);
        return _sl_controller.add_service_level(service_level_name, slo).then([this, service_level_name] () {
            if (_sl_controller.has_service_level(service_level_name)) {
                return make_ready_future<scheduling_group>(_sl_controller.get_scheduling_group(service_level_name));
            } else {
                // The code until here is best effort, to provide fast catchup in case the configuration changes very quickly and being used
                // before this node caught up, or alternatively during startup while the configuration table hasn't been consulted yet.
                // If for some reason we couldn't add the service level, it is better to wait for the configuration to settle,
                // this occasion should be rare enough, even if it happen, two paths are possible, either the initiating node will
                // catch up, figure out the service level has been deleted and will not reattempt this rpc connection, or that this node will
                // eventually catch up with the correct configuration (mainly some service levels that have been deleted and "made room" for this service level) and
                // will eventually allow the connection.
                return make_exception_future<scheduling_group>(std::runtime_error(fmt::format("Rejecting RPC connection for service level: {}, probably only a transitional effect", service_level_name)));
            }
        });
    }
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

void messaging_service::init_feature_listeners() {
    _maintenance_tenant_enabled_listener = _feature_service.maintenance_tenant.when_enabled([this] {
        enable_scheduling_tenant("$maintenance");
    });
}

void messaging_service::enable_scheduling_tenant(std::string_view name) {
    for (size_t i = 0; i < _scheduling_config.statement_tenants.size(); ++i) {
        if (_scheduling_config.statement_tenants[i].name == name) {
            _scheduling_config.statement_tenants[i].enabled = true;
            _connection_index_for_tenant[i].enabled = true;
            return;
        }
    }
}

gms::inet_address messaging_service::get_public_endpoint_for(const gms::inet_address& ip) const {
    auto i = _preferred_to_endpoint.find(ip);
    return i != _preferred_to_endpoint.end() ? i->second : ip;
}

shared_ptr<messaging_service::rpc_protocol_client_wrapper> messaging_service::get_rpc_client(messaging_verb verb, msg_addr id, std::optional<locator::host_id> host_id) {
    SCYLLA_ASSERT(!_shutting_down);
    if (_cfg.maintenance_mode) {
        on_internal_error(mlogger, "This node is in maintenance mode, it shouldn't contact other nodes");
    }
    auto idx = get_rpc_client_idx(verb);
    auto find_existing = [idx, this] (auto& clients, auto hid) -> shared_ptr<rpc_protocol_client_wrapper> {
        auto it = clients[idx].find(hid);

        if (it != clients[idx].end()) {
            auto c = it->second.rpc_client;
            if (!c->error()) {
                return c;
            }
            // The 'dead_only' it should be true, because we're interested in
            // dropping the errored socket, but since it's errored anyway (the
            // above if) it's false to save unneeded second c->error() call
            find_and_remove_client(clients[idx], hid, [] (const auto&) { return true; });
        }
        return nullptr;
    };

    shared_ptr<rpc_protocol_client_wrapper> client;
    if (host_id) {
        client = find_existing(_clients_with_host_id, *host_id);
    } else {
        client = find_existing(_clients, id);
    }

    if (client) {
        return client;
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
        if (_cfg.encrypt == encrypt_what::all || _cfg.encrypt == encrypt_what::transitional || idx == TOPOLOGY_INDEPENDENT_IDX) {
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
        opts.compressor_factory = &_compressor_factory_wrapper->get_factory();
    }
    opts.tcp_nodelay = must_tcp_nodelay;
    opts.reuseaddr = true;
    opts.isolation_cookie = _scheduling_info_for_connection_index[idx].isolation_cookie;
    opts.metrics_domain = client_metrics_domain(idx, id.addr, host_id); // not just `addr` as the latter may be internal IP

    SCYLLA_ASSERT(!must_encrypt || _credentials);

    client = must_encrypt ?
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
    if (host_id) {
        auto res = _clients_with_host_id[idx].emplace(*host_id, shard_info(std::move(client), topology_ignored, id.addr));
        SCYLLA_ASSERT(res.second);
        auto it = res.first;
        client = it->second.rpc_client;
    } else {
        auto res = _clients[idx].emplace(id, shard_info(std::move(client), topology_ignored, id.addr));
        SCYLLA_ASSERT(res.second);
        auto it = res.first;
        client = it->second.rpc_client;
    }
    uint32_t src_cpu_id = this_shard_id();
    // No reply is received, nothing to wait for.
    (void)_rpc->make_client<
            rpc::no_wait_type(gms::inet_address, uint32_t, uint64_t, utils::UUID, std::optional<utils::UUID>)>(messaging_verb::CLIENT_ID)(
                *client, broadcast_address, src_cpu_id,
                query::result_memory_limiter::maximum_result_size, my_host_id.uuid(), host_id ? std::optional{host_id->uuid()} : std::nullopt)
            .handle_exception([ms = shared_from_this(), remote_addr, verb] (std::exception_ptr ep) {
        mlogger.debug("Failed to send client id to {} for verb {}: {}", remote_addr, std::underlying_type_t<messaging_verb>(verb), ep);
    });
    return client;
}

template <typename Fn, typename Map>
requires (std::is_invocable_r_v<bool, Fn, const messaging_service::shard_info&> &&
        (std::is_same_v<typename Map::key_type, msg_addr> || std::is_same_v<typename Map::key_type, locator::host_id>))
void messaging_service::find_and_remove_client(Map& clients, typename Map::key_type id, Fn&& filter) {
    if (_shutting_down) {
        // if messaging service is in a processed of been stopped no need to
        // stop and remove connection here since they are being stopped already
        // and we'll just interfere
        return;
    }

    auto it = clients.find(id);
    if (it != clients.end() && filter(it->second)) {
        auto client = std::move(it->second.rpc_client);

        gms::inet_address addr;
        std::optional<locator::host_id> hid;
        if constexpr (std::is_same_v<typename Map::key_type, msg_addr>) {
            addr = id.addr;
        } else {
            addr = it->second.endpoint;
            hid = id;
        }

        clients.erase(it);

        //
        // Explicitly call rpc_protocol_client_wrapper::stop() for the erased
        // item and hold the messaging_service shared pointer till it's over.
        // This will make sure messaging_service::stop() blocks until
        // client->stop() is over.
        //
        (void)client->stop().finally([addr, client, ms = shared_from_this()] {
            mlogger.debug("dropped connection to {}", addr);
        }).discard_result();
        _connection_dropped(addr, hid);
    }
}

void messaging_service::remove_error_rpc_client(messaging_verb verb, msg_addr id) {
    find_and_remove_client(_clients[get_rpc_client_idx(verb)], id, [] (const auto& s) { return s.rpc_client->error(); });
}

void messaging_service::remove_error_rpc_client(messaging_verb verb, locator::host_id id) {
    find_and_remove_client(_clients_with_host_id[get_rpc_client_idx(verb)], id, [] (const auto& s) { return s.rpc_client->error(); });
}

// Removes client to id.addr in both _client and _clients_with_host_id
// FIXME: make removing from _clients_with_host_id more efficient
void messaging_service::remove_rpc_client(msg_addr id) {
    for (auto& c : _clients) {
        find_and_remove_client(c, id, [] (const auto&) { return true; });
    }
    for (auto& c : _clients_with_host_id) {
        for (auto it = c.begin(); it != c.end();) {
            auto& [hid, si] = *it++;
            if (id.addr == si.endpoint) {
                find_and_remove_client(c, hid, [] (const auto&) { return true; });
            }
        }
    }
}

void messaging_service::remove_rpc_client_with_ignored_topology(msg_addr id, locator::host_id hid) {
    for (auto& c : _clients) {
        find_and_remove_client(c, id, [id] (const auto& s) {
            if (s.topology_ignored) {
                mlogger.info("Dropping connection to {} because it was created without topology information", id.addr);
            }
            return s.topology_ignored;
        });
    }
    for (auto& c : _clients_with_host_id) {
        find_and_remove_client(c, hid, [hid] (const auto& s) {
            if (s.topology_ignored) {
                mlogger.info("Dropping connection to {} because it was created without topology information", hid);
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
messaging_service::make_sink_and_source_for_stream_mutation_fragments(table_schema_version schema_id, streaming::plan_id plan_id, table_id cf_id, uint64_t estimated_partitions, streaming::stream_reason reason, service::session_id session, locator::host_id id) {
    using value_type = std::tuple<rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, rpc::source<int32_t>>;
    if (is_shutting_down()) {
        return make_exception_future<value_type>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(messaging_verb::STREAM_MUTATION_FRAGMENTS, addr_for_host_id(id), id);
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
messaging_service::make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, locator::host_id id) {
    auto verb = messaging_verb::REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM;
    if (is_shutting_down()) {
        return make_exception_future<std::tuple<rpc::sink<repair_hash_with_cmd>, rpc::source<repair_row_on_wire_with_cmd>>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, addr_for_host_id(id), id);
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
messaging_service::make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, locator::host_id id) {
    auto verb = messaging_verb::REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM;
    if (is_shutting_down()) {
        return make_exception_future<std::tuple<rpc::sink<repair_row_on_wire_with_cmd>, rpc::source<repair_stream_cmd>>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, addr_for_host_id(id), id);
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
messaging_service::make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(uint32_t repair_meta_id, shard_id dst_cpu_id, locator::host_id id) {
    auto verb = messaging_verb::REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM;
    if (is_shutting_down()) {
        return make_exception_future<std::tuple<rpc::sink<repair_stream_cmd>, rpc::source<repair_hash_with_cmd>>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, addr_for_host_id(id), id);
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

unsigned messaging_service::add_statement_tenant(sstring tenant_name, scheduling_group sg) {
    auto idx = _clients.size();
    auto scheduling_info_for_connection_index_size = _scheduling_info_for_connection_index.size();
    auto undo = defer([&] {
        _clients.resize(idx);
        _clients_with_host_id.resize(idx);
        _scheduling_info_for_connection_index.resize(scheduling_info_for_connection_index_size);
    });
    _clients.resize(_clients.size() + PER_TENANT_CONNECTION_COUNT);
    _clients_with_host_id.resize(_clients_with_host_id.size() + PER_TENANT_CONNECTION_COUNT);
    // this functions as a way to delete an obsolete tenant with the same name but keeping _clients
    // indexing and _scheduling_info_for_connection_index indexing in sync.
    sstring first_cookie = sstring(_connection_types_prefix[0]) + tenant_name;
    for (unsigned i = 0; i < _scheduling_info_for_connection_index.size(); i++) {
        if (_scheduling_info_for_connection_index[i].isolation_cookie == first_cookie) {
            // remove all connections associated with this tenant, since we are reinserting it.
            for (size_t j = 0; j < _connection_types_prefix.size() ; j++) {
                _scheduling_info_for_connection_index[i + j].isolation_cookie = "";
            }
            break;
        }
    }
    for (auto&& connection_prefix : _connection_types_prefix) {
        sstring isolation_cookie = sstring(connection_prefix) + tenant_name;
        _scheduling_info_for_connection_index.emplace_back(scheduling_info_for_connection_index{sg, isolation_cookie});
    }
    _dynamic_tenants_to_client_idx.insert_or_assign(tenant_name, idx);
    undo.cancel();
    return idx;
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
