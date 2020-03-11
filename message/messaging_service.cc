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

#include "message/messaging_service.hh"
#include <seastar/core/distributed.hh>
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/inet_address_serializer.hh"
#include "service/storage_service.hh"
#include "streaming/prepare_message.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "gms/gossiper.hh"
#include "query-request.hh"
#include "query-result.hh"
#include <seastar/rpc/rpc.hh>
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "db/view/view_update_backlog.hh"
#include "dht/i_partitioner.hh"
#include "range.hh"
#include "frozen_schema.hh"
#include "repair/repair.hh"
#include "digest_algorithm.hh"
#include "service/paxos/proposal.hh"
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
#include "idl/partition_checksum.dist.hh"
#include "idl/query.dist.hh"
#include "idl/cache_temperature.dist.hh"
#include "idl/view.dist.hh"
#include "idl/mutation.dist.hh"
#include "idl/messaging_service.dist.hh"
#include "idl/paxos.dist.hh"
#include "serializer_impl.hh"
#include "serialization_visitors.hh"
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
#include "idl/partition_checksum.dist.impl.hh"
#include "idl/query.dist.impl.hh"
#include "idl/cache_temperature.dist.impl.hh"
#include "idl/mutation.dist.impl.hh"
#include "idl/messaging_service.dist.impl.hh"
#include "idl/paxos.dist.impl.hh"
#include <seastar/rpc/lz4_compressor.hh>
#include <seastar/rpc/lz4_fragmented_compressor.hh>
#include <seastar/rpc/multi_algo_compressor_factory.hh>
#include "idl/view.dist.impl.hh"
#include "partition_range_compat.hh"
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/indirected.hpp>
#include "frozen_mutation.hh"
#include "flat_mutation_reader.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"

namespace netw {

// thunk from rpc serializers to generate serializers
template <typename T, typename Output>
void write(serializer, Output& out, const T& data) {
    ser::serialize(out, data);
}
template <typename T, typename Input>
T read(serializer, Input& in, boost::type<T> type) {
    return ser::deserialize(in, type);
}

template <typename Output, typename T>
void write(serializer s, Output& out, const foreign_ptr<T>& v) {
    return write(s, out, *v);
}
template <typename Input, typename T>
foreign_ptr<T> read(serializer s, Input& in, boost::type<foreign_ptr<T>>) {
    return make_foreign(read(s, in, boost::type<T>()));
}

template <typename Output, typename T>
void write(serializer s, Output& out, const lw_shared_ptr<T>& v) {
    return write(s, out, *v);
}
template <typename Input, typename T>
lw_shared_ptr<T> read(serializer s, Input& in, boost::type<lw_shared_ptr<T>>) {
    return make_lw_shared(read(s, in, boost::type<T>()));
}

static logging::logger mlogger("messaging_service");
static logging::logger rpc_logger("rpc");

using inet_address = gms::inet_address;
using gossip_digest_syn = gms::gossip_digest_syn;
using gossip_digest_ack = gms::gossip_digest_ack;
using gossip_digest_ack2 = gms::gossip_digest_ack2;
using rpc_protocol = rpc::protocol<serializer, messaging_verb>;
using namespace std::chrono_literals;

static rpc::lz4_fragmented_compressor::factory lz4_fragmented_compressor_factory;
static rpc::lz4_compressor::factory lz4_compressor_factory;
static rpc::multi_algo_compressor_factory compressor_factory {
    &lz4_fragmented_compressor_factory,
    &lz4_compressor_factory,
};

struct messaging_service::rpc_protocol_wrapper : public rpc_protocol { using rpc_protocol::rpc_protocol; };

// This wrapper pretends to be rpc_protocol::client, but also handles
// stopping it before destruction, in case it wasn't stopped already.
// This should be integrated into messaging_service proper.
class messaging_service::rpc_protocol_client_wrapper {
    std::unique_ptr<rpc_protocol::client> _p;
    ::shared_ptr<seastar::tls::server_credentials> _credentials;
public:
    rpc_protocol_client_wrapper(rpc_protocol& proto, rpc::client_options opts, socket_address addr, socket_address local = {})
            : _p(std::make_unique<rpc_protocol::client>(proto, std::move(opts), addr, local)) {
    }
    rpc_protocol_client_wrapper(rpc_protocol& proto, rpc::client_options opts, socket_address addr, socket_address local, ::shared_ptr<seastar::tls::server_credentials> c)
            : _p(std::make_unique<rpc_protocol::client>(proto, std::move(opts), seastar::tls::socket(c), addr, local))
            , _credentials(c)
    {}
    auto get_stats() const { return _p->get_stats(); }
    future<> stop() { return _p->stop(); }
    bool error() {
        return _p->error();
    }
    operator rpc_protocol::client&() { return *_p; }

    /**
     * #3787 Must ensure we use the right type of socker. I.e. tls or not.
     * See above, we retain credentials object so we here can know if we
     * are tls or not.
     */
    template<typename Serializer, typename... Out>
    future<rpc::sink<Out...>> make_stream_sink() {
        if (_credentials) {
            return _p->make_stream_sink<Serializer, Out...>(seastar::tls::socket(_credentials));
        }
        return _p->make_stream_sink<Serializer, Out...>();
    }
};

struct messaging_service::rpc_protocol_server_wrapper : public rpc_protocol::server { using rpc_protocol::server::server; };

constexpr int32_t messaging_service::current_version;

distributed<messaging_service> _the_messaging_service;

bool operator==(const msg_addr& x, const msg_addr& y) {
    // Ignore cpu id for now since we do not really support shard to shard connections
    return x.addr == y.addr;
}

bool operator<(const msg_addr& x, const msg_addr& y) {
    // Ignore cpu id for now since we do not really support shard to shard connections
    if (x.addr < y.addr) {
        return true;
    } else {
        return false;
    }
}

std::ostream& operator<<(std::ostream& os, const msg_addr& x) {
    return os << x.addr << ":" << x.cpu_id;
}

size_t msg_addr::hash::operator()(const msg_addr& id) const {
    // Ignore cpu id for now since we do not really support // shard to shard connections
    return std::hash<bytes_view>()(id.addr.bytes());
}

messaging_service::shard_info::shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client)
    : rpc_client(std::move(client)) {
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

messaging_service::drop_notifier_handler messaging_service::register_connection_drop_notifier(std::function<void(gms::inet_address ep)> cb) {
    _connection_drop_notifiers.push_back(std::move(cb));
    return _connection_drop_notifiers.end();
}

void messaging_service::unregister_connection_drop_notifier(messaging_service::drop_notifier_handler h) {
    _connection_drop_notifiers.erase(h);
}

int32_t messaging_service::get_raw_version(const gms::inet_address& endpoint) const {
    // FIXME: messaging service versioning
    return current_version;
}

bool messaging_service::knows_version(const gms::inet_address& endpoint) const {
    // FIXME: messaging service versioning
    return true;
}

// Register a handler (a callback lambda) for verb
template <typename Func>
void register_handler(messaging_service* ms, messaging_verb verb, Func&& func) {
    ms->rpc()->register_handler(verb, ms->scheduling_group_for_verb(verb), std::move(func));
}

future<> messaging_service::unregister_handler(messaging_verb verb) {
    return _rpc->unregister_handler(verb);
}

messaging_service::messaging_service(gms::inet_address ip, uint16_t port, bool listen_now)
    : messaging_service(std::move(ip), port, encrypt_what::none, compress_what::none, tcp_nodelay_what::all, 0, nullptr, memory_config{1'000'000},
            scheduling_config{}, false, listen_now)
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

void messaging_service::start_listen() {
    bool listen_to_bc = _should_listen_to_broadcast_address && _listen_address != utils::fb_utilities::get_broadcast_address();
    rpc::server_options so;
    if (_compress_what != compress_what::none) {
        so.compressor_factory = &compressor_factory;
    }
    so.load_balancing_algorithm = server_socket::load_balancing_algorithm::port;

    // FIXME: we don't set so.tcp_nodelay, because we can't tell at this point whether the connection will come from a
    //        local or remote datacenter, and whether or not the connection will be used for gossip. We can fix
    //        the first by wrapping its server_socket, but not the second.
    auto limits = rpc_resource_limits(_mcfg.rpc_memory_limit);
    if (!_server[0]) {
        auto listen = [&] (const gms::inet_address& a, rpc::streaming_domain_type sdomain) {
            so.streaming_domain = sdomain;
            auto addr = socket_address{a, _port};
            return std::unique_ptr<rpc_protocol_server_wrapper>(new rpc_protocol_server_wrapper(*_rpc,
                    so, addr, limits));
        };
        _server[0] = listen(_listen_address, rpc::streaming_domain_type(0x55AA));
        if (listen_to_bc) {
            _server[1] = listen(utils::fb_utilities::get_broadcast_address(), rpc::streaming_domain_type(0x66BB));
        }
    }

    if (!_server_tls[0]) {
        auto listen = [&] (const gms::inet_address& a, rpc::streaming_domain_type sdomain) {
            so.streaming_domain = sdomain;
            return std::unique_ptr<rpc_protocol_server_wrapper>(
                    [this, &so, &a, limits] () -> std::unique_ptr<rpc_protocol_server_wrapper>{
                if (_encrypt_what == encrypt_what::none) {
                    return nullptr;
                }
                listen_options lo;
                lo.reuse_address = true;
                lo.lba =  server_socket::load_balancing_algorithm::port;
                auto addr = socket_address{a, _ssl_port};
                return std::make_unique<rpc_protocol_server_wrapper>(*_rpc,
                        so, seastar::tls::listen(_credentials, addr, lo), limits);
            }());
        };
        _server_tls[0] = listen(_listen_address, rpc::streaming_domain_type(0x77CC));
        if (listen_to_bc) {
            _server_tls[1] = listen(utils::fb_utilities::get_broadcast_address(), rpc::streaming_domain_type(0x88DD));
        }
    }
    // Do this on just cpu 0, to avoid duplicate logs.
    if (engine().cpu_id() == 0) {
        if (_server_tls[0]) {
            mlogger.info("Starting Encrypted Messaging Service on SSL port {}", _ssl_port);
        }
        mlogger.info("Starting Messaging Service on port {}", _port);
    }
}

messaging_service::messaging_service(gms::inet_address ip
        , uint16_t port
        , encrypt_what ew
        , compress_what cw
        , tcp_nodelay_what tnw
        , uint16_t ssl_port
        , std::shared_ptr<seastar::tls::credentials_builder> credentials
        , messaging_service::memory_config mcfg
        , scheduling_config scfg
        , bool sltba
        , bool listen_now)
    : _listen_address(ip)
    , _port(port)
    , _ssl_port(ssl_port)
    , _encrypt_what(ew)
    , _compress_what(cw)
    , _tcp_nodelay_what(tnw)
    , _should_listen_to_broadcast_address(sltba)
    , _rpc(new rpc_protocol_wrapper(serializer { }))
    , _credentials(credentials ? credentials->build_server_credentials() : nullptr)
    , _mcfg(mcfg)
    , _scheduling_config(scfg)
{
    _rpc->set_logger([] (const sstring& log) {
            rpc_logger.info("{}", log);
    });
    register_handler(this, messaging_verb::CLIENT_ID, [] (rpc::client_info& ci, gms::inet_address broadcast_address, uint32_t src_cpu_id, rpc::optional<uint64_t> max_result_size) {
        ci.attach_auxiliary("baddr", broadcast_address);
        ci.attach_auxiliary("src_cpu_id", src_cpu_id);
        ci.attach_auxiliary("max_result_size", max_result_size.value_or(query::result_memory_limiter::maximum_result_size));
        return rpc::no_wait;
    });

    if (listen_now) {
        start_listen();
    }
}

msg_addr messaging_service::get_source(const rpc::client_info& cinfo) {
    return msg_addr{
        cinfo.retrieve_auxiliary<gms::inet_address>("baddr"),
        cinfo.retrieve_auxiliary<uint32_t>("src_cpu_id")
    };
}

messaging_service::~messaging_service() = default;

uint16_t messaging_service::port() {
    return _port;
}

gms::inet_address messaging_service::listen_address() {
    return _listen_address;
}

static future<> stop_servers(std::array<std::unique_ptr<messaging_service::rpc_protocol_server_wrapper>, 2>& servers) {
    return parallel_for_each(
            servers | boost::adaptors::filtered([] (auto& ptr) { return bool(ptr); }) | boost::adaptors::indirected,
            std::mem_fn(&messaging_service::rpc_protocol_server_wrapper::stop));
}

future<> messaging_service::stop_tls_server() {
    return stop_servers(_server_tls);
}

future<> messaging_service::stop_nontls_server() {
    return stop_servers(_server);
}

future<> messaging_service::stop_client() {
    return parallel_for_each(_clients, [] (auto& m) {
        return parallel_for_each(m, [] (std::pair<const msg_addr, shard_info>& c) {
            return c.second.rpc_client->stop();
        });
    });
}

future<> messaging_service::stop() {
    _stopping = true;
    return when_all(stop_nontls_server(), stop_tls_server(), stop_client()).discard_result();
}

rpc::no_wait_type messaging_service::no_wait() {
    return rpc::no_wait;
}


static constexpr unsigned do_get_rpc_client_idx(messaging_verb verb) {
    switch (verb) {
    case messaging_verb::CLIENT_ID:
    case messaging_verb::MUTATION:
    case messaging_verb::READ_DATA:
    case messaging_verb::READ_MUTATION_DATA:
    case messaging_verb::READ_DIGEST:
    case messaging_verb::GOSSIP_DIGEST_ACK:
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
        return 0;
    // GET_SCHEMA_VERSION is sent from read/mutate verbs so should be
    // sent on a different connection to avoid potential deadlocks
    // as well as reduce latency as there are potentially many requests
    // blocked on schema version request.
    case messaging_verb::GOSSIP_DIGEST_SYN:
    case messaging_verb::GOSSIP_DIGEST_ACK2:
    case messaging_verb::GOSSIP_SHUTDOWN:
    case messaging_verb::GOSSIP_ECHO:
    case messaging_verb::GET_SCHEMA_VERSION:
        return 1;
    case messaging_verb::PREPARE_MESSAGE:
    case messaging_verb::PREPARE_DONE_MESSAGE:
    case messaging_verb::STREAM_MUTATION:
    case messaging_verb::STREAM_MUTATION_DONE:
    case messaging_verb::COMPLETE_MESSAGE:
    case messaging_verb::REPLICATION_FINISHED:
    case messaging_verb::REPAIR_CHECKSUM_RANGE:
    case messaging_verb::STREAM_MUTATION_FRAGMENTS:
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
    case messaging_verb::HINT_MUTATION:
        return 2;
    case messaging_verb::MUTATION_DONE:
    case messaging_verb::MUTATION_FAILED:
        return 3;
    case messaging_verb::LAST:
        return -1; // should never happen
    }
}

static constexpr std::array<uint8_t, static_cast<size_t>(messaging_verb::LAST)> make_rpc_client_idx_table() {
    std::array<uint8_t, static_cast<size_t>(messaging_verb::LAST)> tab{};
    for (size_t i = 0; i < tab.size(); ++i) {
        tab[i] = do_get_rpc_client_idx(messaging_verb(i));
    }
    return tab;
}

static std::array<uint8_t, static_cast<size_t>(messaging_verb::LAST)> s_rpc_client_idx_table = make_rpc_client_idx_table();

static unsigned get_rpc_client_idx(messaging_verb verb) {
    return s_rpc_client_idx_table[static_cast<size_t>(verb)];
}

scheduling_group
messaging_service::scheduling_group_for_verb(messaging_verb verb) const {
    static const scheduling_group scheduling_config::*idx_to_group[] = {
        &scheduling_config::statement,
        &scheduling_config::gossip,
        &scheduling_config::streaming,
        &scheduling_config::statement,
    };
    return _scheduling_config.*(idx_to_group[get_rpc_client_idx(verb)]);
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
        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
        auto my_addr = utils::fb_utilities::get_broadcast_address();

        if (snitch_ptr->get_datacenter(ep) == snitch_ptr->get_datacenter(my_addr)) {
            return it->second;
        }
    }

    // If cache doesn't have an entry for this endpoint - return endpoint itself
    return ep;
}

future<> messaging_service::init_local_preferred_ip_cache() {
    return db::system_keyspace::get_preferred_ips().then([this] (auto ips_cache) {
        _preferred_ip_cache = ips_cache;
        //
        // Reset the connections to the endpoints that have entries in
        // _preferred_ip_cache so that they reopen with the preferred IPs we've
        // just read.
        //
        for (auto& p : _preferred_ip_cache) {
            this->remove_rpc_client(msg_addr(p.first));
        }
    });
}

void messaging_service::cache_preferred_ip(gms::inet_address ep, gms::inet_address ip) {
    _preferred_ip_cache[ep] = ip;
}

shared_ptr<messaging_service::rpc_protocol_client_wrapper> messaging_service::get_rpc_client(messaging_verb verb, msg_addr id) {
    assert(!_stopping);
    auto idx = get_rpc_client_idx(verb);
    auto it = _clients[idx].find(id);

    if (it != _clients[idx].end()) {
        auto c = it->second.rpc_client;
        if (!c->error()) {
            return c;
        }
        remove_error_rpc_client(verb, id);
    }

    auto must_encrypt = [&id, this] {
        if (_encrypt_what == encrypt_what::none) {
            return false;
        }
        if (_encrypt_what == encrypt_what::all) {
            return true;
        }

        auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();

        if (_encrypt_what == encrypt_what::dc) {
            return snitch_ptr->get_datacenter(id.addr)
                            != snitch_ptr->get_datacenter(utils::fb_utilities::get_broadcast_address());
        }
        return snitch_ptr->get_rack(id.addr)
                        != snitch_ptr->get_rack(utils::fb_utilities::get_broadcast_address());
    }();

    auto must_compress = [&id, this] {
        if (_compress_what == compress_what::none) {
            return false;
        }

        if (_compress_what == compress_what::dc) {
            auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
            return snitch_ptr->get_datacenter(id.addr)
                            != snitch_ptr->get_datacenter(utils::fb_utilities::get_broadcast_address());
        }

        return true;
    }();

    auto must_tcp_nodelay = [&] {
        if (idx == 1) {
            return true; // gossip
        }
        if (_tcp_nodelay_what == tcp_nodelay_what::local) {
            auto& snitch_ptr = locator::i_endpoint_snitch::get_local_snitch_ptr();
            return snitch_ptr->get_datacenter(id.addr)
                            == snitch_ptr->get_datacenter(utils::fb_utilities::get_broadcast_address());
        }
        return true;
    }();

    auto remote_addr = socket_address(get_preferred_ip(id.addr), must_encrypt ? _ssl_port : _port);

    rpc::client_options opts;
    // send keepalive messages each minute if connection is idle, drop connection after 10 failures
    opts.keepalive = std::optional<net::tcp_keepalive_params>({60s, 60s, 10});
    if (must_compress) {
        opts.compressor_factory = &compressor_factory;
    }
    opts.tcp_nodelay = must_tcp_nodelay;
    opts.reuseaddr = true;

    auto client = must_encrypt ?
                    ::make_shared<rpc_protocol_client_wrapper>(*_rpc, std::move(opts),
                                    remote_addr, socket_address(), _credentials) :
                    ::make_shared<rpc_protocol_client_wrapper>(*_rpc, std::move(opts),
                                    remote_addr);

    auto res = _clients[idx].emplace(id, shard_info(std::move(client)));
    assert(res.second);
    it = res.first;
    uint32_t src_cpu_id = engine().cpu_id();
    // No reply is received, nothing to wait for.
    (void)_rpc->make_client<rpc::no_wait_type(gms::inet_address, uint32_t, uint64_t)>(messaging_verb::CLIENT_ID)(*it->second.rpc_client, utils::fb_utilities::get_broadcast_address(), src_cpu_id,
                                                                                                           query::result_memory_limiter::maximum_result_size).handle_exception([ms = shared_from_this(), remote_addr, verb] (std::exception_ptr ep) {
        mlogger.debug("Failed to send client id to {} for verb {}: {}", remote_addr, std::underlying_type_t<messaging_verb>(verb), ep);
    });
    return it->second.rpc_client;
}

bool messaging_service::remove_rpc_client_one(clients_map& clients, msg_addr id, bool dead_only) {
    if (_stopping) {
        // if messaging service is in a processed of been stopped no need to
        // stop and remove connection here since they are being stopped already
        // and we'll just interfere
        return false;
    }

    bool found = false;
    auto it = clients.find(id);
    if (it != clients.end() && (!dead_only || it->second.rpc_client->error())) {
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
        found = true;
    }
    return found;
}

void messaging_service::remove_error_rpc_client(messaging_verb verb, msg_addr id) {
    if (remove_rpc_client_one(_clients[get_rpc_client_idx(verb)], id, true)) {
        for (auto&& cb : _connection_drop_notifiers) {
            cb(id.addr);
        }
    }
}

void messaging_service::remove_rpc_client(msg_addr id) {
    for (auto& c : _clients) {
        remove_rpc_client_one(c, id, false);
    }
}

std::unique_ptr<messaging_service::rpc_protocol_wrapper>& messaging_service::rpc() {
    return _rpc;
}

rpc::sink<int32_t> messaging_service::make_sink_for_stream_mutation_fragments(rpc::source<frozen_mutation_fragment, rpc::optional<streaming::stream_mutation_fragments_cmd>>& source) {
    return source.make_sink<netw::serializer, int32_t>();
}

future<rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, rpc::source<int32_t>>
messaging_service::make_sink_and_source_for_stream_mutation_fragments(utils::UUID schema_id, utils::UUID plan_id, utils::UUID cf_id, uint64_t estimated_partitions, streaming::stream_reason reason, msg_addr id) {
    if (is_stopping()) {
        return make_exception_future<rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, rpc::source<int32_t>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(messaging_verb::STREAM_MUTATION_FRAGMENTS, id);
    return rpc_client->make_stream_sink<netw::serializer, frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>().then([this, plan_id, schema_id, cf_id, estimated_partitions, reason, rpc_client] (rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd> sink) mutable {
        auto rpc_handler = rpc()->make_client<rpc::source<int32_t> (utils::UUID, utils::UUID, utils::UUID, uint64_t, streaming::stream_reason, rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>)>(messaging_verb::STREAM_MUTATION_FRAGMENTS);
        return rpc_handler(*rpc_client , plan_id, schema_id, cf_id, estimated_partitions, reason, sink).then_wrapped([sink, rpc_client] (future<rpc::source<int32_t>> source) mutable {
            return (source.failed() ? sink.close() : make_ready_future<>()).then([sink = std::move(sink), source = std::move(source)] () mutable {
                return make_ready_future<rpc::sink<frozen_mutation_fragment, streaming::stream_mutation_fragments_cmd>, rpc::source<int32_t>>(std::move(sink), std::move(source.get0()));
            });
        });
    });
}

void messaging_service::register_stream_mutation_fragments(std::function<future<rpc::sink<int32_t>> (const rpc::client_info& cinfo, UUID plan_id, UUID schema_id, UUID cf_id, uint64_t estimated_partitions, rpc::optional<streaming::stream_reason>, rpc::source<frozen_mutation_fragment, rpc::optional<streaming::stream_mutation_fragments_cmd>> source)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION_FRAGMENTS, std::move(func));
}

template<class SinkType, class SourceType>
future<rpc::sink<SinkType>, rpc::source<SourceType>>
do_make_sink_source(messaging_verb verb, uint32_t repair_meta_id, shared_ptr<messaging_service::rpc_protocol_client_wrapper> rpc_client, std::unique_ptr<messaging_service::rpc_protocol_wrapper>& rpc) {
    return rpc_client->make_stream_sink<netw::serializer, SinkType>().then([&rpc, verb, repair_meta_id, rpc_client] (rpc::sink<SinkType> sink) mutable {
        auto rpc_handler = rpc->make_client<rpc::source<SourceType> (uint32_t, rpc::sink<SinkType>)>(verb);
        return rpc_handler(*rpc_client, repair_meta_id, sink).then_wrapped([sink, rpc_client] (future<rpc::source<SourceType>> source) mutable {
            return (source.failed() ? sink.close() : make_ready_future<>()).then([sink = std::move(sink), source = std::move(source)] () mutable {
                return make_ready_future<rpc::sink<SinkType>, rpc::source<SourceType>>(std::move(sink), std::move(source.get0()));
            });
        });
    });
}

// Wrapper for REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM
future<rpc::sink<repair_hash_with_cmd>, rpc::source<repair_row_on_wire_with_cmd>>
messaging_service::make_sink_and_source_for_repair_get_row_diff_with_rpc_stream(uint32_t repair_meta_id, msg_addr id) {
    auto verb = messaging_verb::REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM;
    if (is_stopping()) {
        return make_exception_future<rpc::sink<repair_hash_with_cmd>, rpc::source<repair_row_on_wire_with_cmd>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, id);
    return do_make_sink_source<repair_hash_with_cmd, repair_row_on_wire_with_cmd>(verb, repair_meta_id, std::move(rpc_client), rpc());
}

rpc::sink<repair_row_on_wire_with_cmd> messaging_service::make_sink_for_repair_get_row_diff_with_rpc_stream(rpc::source<repair_hash_with_cmd>& source) {
    return source.make_sink<netw::serializer, repair_row_on_wire_with_cmd>();
}

void messaging_service::register_repair_get_row_diff_with_rpc_stream(std::function<future<rpc::sink<repair_row_on_wire_with_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_hash_with_cmd> source)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_ROW_DIFF_WITH_RPC_STREAM, std::move(func));
}

// Wrapper for REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM
future<rpc::sink<repair_row_on_wire_with_cmd>, rpc::source<repair_stream_cmd>>
messaging_service::make_sink_and_source_for_repair_put_row_diff_with_rpc_stream(uint32_t repair_meta_id, msg_addr id) {
    auto verb = messaging_verb::REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM;
    if (is_stopping()) {
        return make_exception_future<rpc::sink<repair_row_on_wire_with_cmd>, rpc::source<repair_stream_cmd>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, id);
    return do_make_sink_source<repair_row_on_wire_with_cmd, repair_stream_cmd>(verb, repair_meta_id, std::move(rpc_client), rpc());
}

rpc::sink<repair_stream_cmd> messaging_service::make_sink_for_repair_put_row_diff_with_rpc_stream(rpc::source<repair_row_on_wire_with_cmd>& source) {
    return source.make_sink<netw::serializer, repair_stream_cmd>();
}

void messaging_service::register_repair_put_row_diff_with_rpc_stream(std::function<future<rpc::sink<repair_stream_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_row_on_wire_with_cmd> source)>&& func) {
    register_handler(this, messaging_verb::REPAIR_PUT_ROW_DIFF_WITH_RPC_STREAM, std::move(func));
}

// Wrapper for REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM
future<rpc::sink<repair_stream_cmd>, rpc::source<repair_hash_with_cmd>>
messaging_service::make_sink_and_source_for_repair_get_full_row_hashes_with_rpc_stream(uint32_t repair_meta_id, msg_addr id) {
    auto verb = messaging_verb::REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM;
    if (is_stopping()) {
        return make_exception_future<rpc::sink<repair_stream_cmd>, rpc::source<repair_hash_with_cmd>>(rpc::closed_error());
    }
    auto rpc_client = get_rpc_client(verb, id);
    return do_make_sink_source<repair_stream_cmd, repair_hash_with_cmd>(verb, repair_meta_id, std::move(rpc_client), rpc());
}

rpc::sink<repair_hash_with_cmd> messaging_service::make_sink_for_repair_get_full_row_hashes_with_rpc_stream(rpc::source<repair_stream_cmd>& source) {
    return source.make_sink<netw::serializer, repair_hash_with_cmd>();
}

void messaging_service::register_repair_get_full_row_hashes_with_rpc_stream(std::function<future<rpc::sink<repair_hash_with_cmd>> (const rpc::client_info& cinfo, uint32_t repair_meta_id, rpc::source<repair_stream_cmd> source)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_FULL_ROW_HASHES_WITH_RPC_STREAM, std::move(func));
}

// Send a message for verb
template <typename MsgIn, typename... MsgOut>
auto send_message(messaging_service* ms, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    auto rpc_handler = ms->rpc()->make_client<MsgIn(MsgOut...)>(verb);
    if (ms->is_stopping()) {
        using futurator = futurize<std::result_of_t<decltype(rpc_handler)(rpc_protocol::client&, MsgOut...)>>;
        return futurator::make_exception_future(rpc::closed_error());
    }
    auto rpc_client_ptr = ms->get_rpc_client(verb, id);
    auto& rpc_client = *rpc_client_ptr;
    return rpc_handler(rpc_client, std::forward<MsgOut>(msg)...).then_wrapped([ms = ms->shared_from_this(), id, verb, rpc_client_ptr = std::move(rpc_client_ptr)] (auto&& f) {
        try {
            if (f.failed()) {
                ms->increment_dropped_messages(verb);
                f.get();
                assert(false); // never reached
            }
            return std::move(f);
        } catch (rpc::closed_error&) {
            // This is a transport error
            ms->remove_error_rpc_client(verb, id);
            throw;
        } catch (...) {
            // This is expected to be a rpc server error, e.g., the rpc handler throws a std::runtime_error.
            throw;
        }
    });
}

// TODO: Remove duplicated code in send_message
template <typename MsgIn, typename Timeout, typename... MsgOut>
auto send_message_timeout(messaging_service* ms, messaging_verb verb, msg_addr id, Timeout timeout, MsgOut&&... msg) {
    auto rpc_handler = ms->rpc()->make_client<MsgIn(MsgOut...)>(verb);
    if (ms->is_stopping()) {
        using futurator = futurize<std::result_of_t<decltype(rpc_handler)(rpc_protocol::client&, MsgOut...)>>;
        return futurator::make_exception_future(rpc::closed_error());
    }
    auto rpc_client_ptr = ms->get_rpc_client(verb, id);
    auto& rpc_client = *rpc_client_ptr;
    return rpc_handler(rpc_client, timeout, std::forward<MsgOut>(msg)...).then_wrapped([ms = ms->shared_from_this(), id, verb, rpc_client_ptr = std::move(rpc_client_ptr)] (auto&& f) {
        try {
            if (f.failed()) {
                ms->increment_dropped_messages(verb);
                f.get();
                assert(false); // never reached
            }
            return std::move(f);
        } catch (rpc::closed_error&) {
            // This is a transport error
            ms->remove_error_rpc_client(verb, id);
            throw;
        } catch (...) {
            // This is expected to be a rpc server error, e.g., the rpc handler throws a std::runtime_error.
            throw;
        }
    });
}

// Send one way message for verb
template <typename... MsgOut>
auto send_message_oneway(messaging_service* ms, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    return send_message<rpc::no_wait_type>(ms, std::move(verb), std::move(id), std::forward<MsgOut>(msg)...);
}

// Send one way message for verb
template <typename Timeout, typename... MsgOut>
auto send_message_oneway_timeout(messaging_service* ms, Timeout timeout, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    return send_message_timeout<rpc::no_wait_type>(ms, std::move(verb), std::move(id), timeout, std::forward<MsgOut>(msg)...);
}

// Wrappers for verbs

// PREPARE_MESSAGE
void messaging_service::register_prepare_message(std::function<future<streaming::prepare_message> (const rpc::client_info& cinfo,
        streaming::prepare_message msg, UUID plan_id, sstring description, rpc::optional<streaming::stream_reason> reason)>&& func) {
    register_handler(this, messaging_verb::PREPARE_MESSAGE, std::move(func));
}
future<streaming::prepare_message> messaging_service::send_prepare_message(msg_addr id, streaming::prepare_message msg, UUID plan_id,
        sstring description, streaming::stream_reason reason) {
    return send_message<streaming::prepare_message>(this, messaging_verb::PREPARE_MESSAGE, id,
        std::move(msg), plan_id, std::move(description), reason);
}

// PREPARE_DONE_MESSAGE
void messaging_service::register_prepare_done_message(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::PREPARE_DONE_MESSAGE, std::move(func));
}
future<> messaging_service::send_prepare_done_message(msg_addr id, UUID plan_id, unsigned dst_cpu_id) {
    return send_message<void>(this, messaging_verb::PREPARE_DONE_MESSAGE, id,
        plan_id, dst_cpu_id);
}

// STREAM_MUTATION
void messaging_service::register_stream_mutation(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id, rpc::optional<bool> fragmented, rpc::optional<streaming::stream_reason> reason)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION, std::move(func));
}
future<> messaging_service::send_stream_mutation(msg_addr id, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id, bool fragmented, streaming::stream_reason reason) {
    return send_message<void>(this, messaging_verb::STREAM_MUTATION, id,
        plan_id, std::move(fm), dst_cpu_id, fragmented, reason);
}

// STREAM_MUTATION_DONE
void messaging_service::register_stream_mutation_done(std::function<future<> (const rpc::client_info& cinfo,
        UUID plan_id, dht::token_range_vector ranges, UUID cf_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION_DONE,
            [func = std::move(func)] (const rpc::client_info& cinfo,
                    UUID plan_id, std::vector<wrapping_range<dht::token>> ranges,
                    UUID cf_id, unsigned dst_cpu_id) mutable {
        return func(cinfo, plan_id, ::compat::unwrap(std::move(ranges)), cf_id, dst_cpu_id);
    });
}
future<> messaging_service::send_stream_mutation_done(msg_addr id, UUID plan_id, dht::token_range_vector ranges, UUID cf_id, unsigned dst_cpu_id) {
    return send_message<void>(this, messaging_verb::STREAM_MUTATION_DONE, id,
        plan_id, std::move(ranges), cf_id, dst_cpu_id);
}

// COMPLETE_MESSAGE
void messaging_service::register_complete_message(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id, rpc::optional<bool> failed)>&& func) {
    register_handler(this, messaging_verb::COMPLETE_MESSAGE, std::move(func));
}
future<> messaging_service::send_complete_message(msg_addr id, UUID plan_id, unsigned dst_cpu_id, bool failed) {
    return send_message<void>(this, messaging_verb::COMPLETE_MESSAGE, id,
        plan_id, dst_cpu_id, failed);
}

void messaging_service::register_gossip_echo(std::function<future<> ()>&& func) {
    register_handler(this, messaging_verb::GOSSIP_ECHO, std::move(func));
}
future<> messaging_service::unregister_gossip_echo() {
    return unregister_handler(netw::messaging_verb::GOSSIP_ECHO);
}
future<> messaging_service::send_gossip_echo(msg_addr id) {
    return send_message_timeout<void>(this, messaging_verb::GOSSIP_ECHO, std::move(id), 3000ms);
}

void messaging_service::register_gossip_shutdown(std::function<rpc::no_wait_type (inet_address from)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_SHUTDOWN, std::move(func));
}
future<> messaging_service::unregister_gossip_shutdown() {
    return unregister_handler(netw::messaging_verb::GOSSIP_SHUTDOWN);
}
future<> messaging_service::send_gossip_shutdown(msg_addr id, inet_address from) {
    return send_message_oneway(this, messaging_verb::GOSSIP_SHUTDOWN, std::move(id), std::move(from));
}

// gossip syn
void messaging_service::register_gossip_digest_syn(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, gossip_digest_syn)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_SYN, std::move(func));
}
future<> messaging_service::unregister_gossip_digest_syn() {
    return unregister_handler(netw::messaging_verb::GOSSIP_DIGEST_SYN);
}
future<> messaging_service::send_gossip_digest_syn(msg_addr id, gossip_digest_syn msg) {
    return send_message_oneway(this, messaging_verb::GOSSIP_DIGEST_SYN, std::move(id), std::move(msg));
}

// gossip ack
void messaging_service::register_gossip_digest_ack(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, gossip_digest_ack)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_ACK, std::move(func));
}
future<> messaging_service::unregister_gossip_digest_ack() {
    return unregister_handler(netw::messaging_verb::GOSSIP_DIGEST_ACK);
}
future<> messaging_service::send_gossip_digest_ack(msg_addr id, gossip_digest_ack msg) {
    return send_message_oneway(this, messaging_verb::GOSSIP_DIGEST_ACK, std::move(id), std::move(msg));
}

// gossip ack2
void messaging_service::register_gossip_digest_ack2(std::function<rpc::no_wait_type (gossip_digest_ack2)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_ACK2, std::move(func));
}
future<> messaging_service::unregister_gossip_digest_ack2() {
    return unregister_handler(netw::messaging_verb::GOSSIP_DIGEST_ACK2);
}
future<> messaging_service::send_gossip_digest_ack2(msg_addr id, gossip_digest_ack2 msg) {
    return send_message_oneway(this, messaging_verb::GOSSIP_DIGEST_ACK2, std::move(id), std::move(msg));
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

void messaging_service::register_mutation(std::function<future<rpc::no_wait_type> (const rpc::client_info&, rpc::opt_time_point, frozen_mutation fm, std::vector<inet_address> forward,
    inet_address reply_to, unsigned shard, response_id_type response_id, rpc::optional<std::optional<tracing::trace_info>> trace_info)>&& func) {
    register_handler(this, netw::messaging_verb::MUTATION, std::move(func));
}
future<> messaging_service::unregister_mutation() {
    return unregister_handler(netw::messaging_verb::MUTATION);
}
future<> messaging_service::send_mutation(msg_addr id, clock_type::time_point timeout, const frozen_mutation& fm, std::vector<inet_address> forward,
    inet_address reply_to, unsigned shard, response_id_type response_id, std::optional<tracing::trace_info> trace_info) {
    return send_message_oneway_timeout(this, timeout, messaging_verb::MUTATION, std::move(id), fm, std::move(forward),
        std::move(reply_to), shard, std::move(response_id), std::move(trace_info));
}

void messaging_service::register_counter_mutation(std::function<future<> (const rpc::client_info&, rpc::opt_time_point, std::vector<frozen_mutation> fms, db::consistency_level cl, std::optional<tracing::trace_info> trace_info)>&& func) {
    register_handler(this, netw::messaging_verb::COUNTER_MUTATION, std::move(func));
}
future<> messaging_service::unregister_counter_mutation() {
    return unregister_handler(netw::messaging_verb::COUNTER_MUTATION);
}
future<> messaging_service::send_counter_mutation(msg_addr id, clock_type::time_point timeout, std::vector<frozen_mutation> fms, db::consistency_level cl, std::optional<tracing::trace_info> trace_info) {
    return send_message_timeout<void>(this, messaging_verb::COUNTER_MUTATION, std::move(id), timeout, std::move(fms), cl, std::move(trace_info));
}

void messaging_service::register_mutation_done(std::function<future<rpc::no_wait_type> (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id, rpc::optional<db::view::update_backlog> backlog)>&& func) {
    register_handler(this, netw::messaging_verb::MUTATION_DONE, std::move(func));
}
future<> messaging_service::unregister_mutation_done() {
    return unregister_handler(netw::messaging_verb::MUTATION_DONE);
}
future<> messaging_service::send_mutation_done(msg_addr id, unsigned shard, response_id_type response_id, db::view::update_backlog backlog) {
    return send_message_oneway(this, messaging_verb::MUTATION_DONE, std::move(id), shard, std::move(response_id), std::move(backlog));
}

void messaging_service::register_mutation_failed(std::function<future<rpc::no_wait_type> (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id, size_t num_failed, rpc::optional<db::view::update_backlog> backlog)>&& func) {
    register_handler(this, netw::messaging_verb::MUTATION_FAILED, std::move(func));
}
future<> messaging_service::unregister_mutation_failed() {
    return unregister_handler(netw::messaging_verb::MUTATION_FAILED);
}
future<> messaging_service::send_mutation_failed(msg_addr id, unsigned shard, response_id_type response_id, size_t num_failed, db::view::update_backlog backlog) {
    return send_message_oneway(this, messaging_verb::MUTATION_FAILED, std::move(id), shard, std::move(response_id), num_failed, std::move(backlog));
}

void messaging_service::register_read_data(std::function<future<rpc::tuple<foreign_ptr<lw_shared_ptr<query::result>>, cache_temperature>> (const rpc::client_info&, rpc::opt_time_point t, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> oda)>&& func) {
    register_handler(this, netw::messaging_verb::READ_DATA, std::move(func));
}
future<> messaging_service::unregister_read_data() {
    return unregister_handler(netw::messaging_verb::READ_DATA);
}
future<rpc::tuple<query::result, rpc::optional<cache_temperature>>> messaging_service::send_read_data(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr, query::digest_algorithm da) {
    return send_message_timeout<future<rpc::tuple<query::result, rpc::optional<cache_temperature>>>>(this, messaging_verb::READ_DATA, std::move(id), timeout, cmd, pr, da);
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

void messaging_service::register_schema_check(std::function<future<utils::UUID>()>&& func) {
    register_handler(this, netw::messaging_verb::SCHEMA_CHECK, std::move(func));
}
future<> messaging_service::unregister_schema_check() {
    return unregister_handler(netw::messaging_verb::SCHEMA_CHECK);
}
future<utils::UUID> messaging_service::send_schema_check(msg_addr dst) {
    return send_message<utils::UUID>(this, netw::messaging_verb::SCHEMA_CHECK, dst);
}

void messaging_service::register_read_mutation_data(std::function<future<rpc::tuple<foreign_ptr<lw_shared_ptr<reconcilable_result>>, cache_temperature>> (const rpc::client_info&, rpc::opt_time_point t, query::read_command cmd, ::compat::wrapping_partition_range pr)>&& func) {
    register_handler(this, netw::messaging_verb::READ_MUTATION_DATA, std::move(func));
}
future<> messaging_service::unregister_read_mutation_data() {
    return unregister_handler(netw::messaging_verb::READ_MUTATION_DATA);
}
future<rpc::tuple<reconcilable_result, rpc::optional<cache_temperature>>> messaging_service::send_read_mutation_data(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr) {
    return send_message_timeout<future<rpc::tuple<reconcilable_result, rpc::optional<cache_temperature>>>>(this, messaging_verb::READ_MUTATION_DATA, std::move(id), timeout, cmd, pr);
}

void messaging_service::register_read_digest(std::function<future<rpc::tuple<query::result_digest, api::timestamp_type, cache_temperature>> (const rpc::client_info&, rpc::opt_time_point timeout, query::read_command cmd, ::compat::wrapping_partition_range pr, rpc::optional<query::digest_algorithm> oda)>&& func) {
    register_handler(this, netw::messaging_verb::READ_DIGEST, std::move(func));
}
future<> messaging_service::unregister_read_digest() {
    return unregister_handler(netw::messaging_verb::READ_DIGEST);
}
future<rpc::tuple<query::result_digest, rpc::optional<api::timestamp_type>, rpc::optional<cache_temperature>>> messaging_service::send_read_digest(msg_addr id, clock_type::time_point timeout, const query::read_command& cmd, const dht::partition_range& pr, query::digest_algorithm da) {
    return send_message_timeout<future<rpc::tuple<query::result_digest, rpc::optional<api::timestamp_type>, rpc::optional<cache_temperature>>>>(this, netw::messaging_verb::READ_DIGEST, std::move(id), timeout, cmd, pr, da);
}

// Wrapper for TRUNCATE
void messaging_service::register_truncate(std::function<future<> (sstring, sstring)>&& func) {
    register_handler(this, netw::messaging_verb::TRUNCATE, std::move(func));
}

future<> messaging_service::unregister_truncate() {
    return unregister_handler(netw::messaging_verb::TRUNCATE);
}

future<> messaging_service::send_truncate(msg_addr id, std::chrono::milliseconds timeout, sstring ks, sstring cf) {
    return send_message_timeout<void>(this, netw::messaging_verb::TRUNCATE, std::move(id), std::move(timeout), std::move(ks), std::move(cf));
}

// Wrapper for REPLICATION_FINISHED
void messaging_service::register_replication_finished(std::function<future<> (inet_address)>&& func) {
    register_handler(this, messaging_verb::REPLICATION_FINISHED, std::move(func));
}
future<> messaging_service::unregister_replication_finished() {
    return unregister_handler(messaging_verb::REPLICATION_FINISHED);
}
future<> messaging_service::send_replication_finished(msg_addr id, inet_address from) {
    // FIXME: getRpcTimeout : conf.request_timeout_in_ms
    return send_message_timeout<void>(this, messaging_verb::REPLICATION_FINISHED, std::move(id), 10000ms, std::move(from));
}

// Wrapper for REPAIR_CHECKSUM_RANGE
void messaging_service::register_repair_checksum_range(
        std::function<future<partition_checksum> (sstring keyspace,
                sstring cf, dht::token_range range, rpc::optional<repair_checksum> hash_version)>&& f) {
    register_handler(this, messaging_verb::REPAIR_CHECKSUM_RANGE, std::move(f));
}
future<> messaging_service::unregister_repair_checksum_range() {
    return unregister_handler(messaging_verb::REPAIR_CHECKSUM_RANGE);
}
future<partition_checksum> messaging_service::send_repair_checksum_range(
        msg_addr id, sstring keyspace, sstring cf, ::dht::token_range range, repair_checksum hash_version)
{
    return send_message<partition_checksum>(this,
            messaging_verb::REPAIR_CHECKSUM_RANGE, std::move(id),
            std::move(keyspace), std::move(cf), std::move(range), hash_version);
}

// Wrapper for REPAIR_GET_FULL_ROW_HASHES
void messaging_service::register_repair_get_full_row_hashes(std::function<future<std::unordered_set<repair_hash>> (const rpc::client_info& cinfo, uint32_t repair_meta_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_FULL_ROW_HASHES, std::move(func));
}
future<> messaging_service::unregister_repair_get_full_row_hashes() {
    return unregister_handler(messaging_verb::REPAIR_GET_FULL_ROW_HASHES);
}
future<std::unordered_set<repair_hash>> messaging_service::send_repair_get_full_row_hashes(msg_addr id, uint32_t repair_meta_id) {
    return send_message<future<std::unordered_set<repair_hash>>>(this, messaging_verb::REPAIR_GET_FULL_ROW_HASHES, std::move(id), repair_meta_id);
}

// Wrapper for REPAIR_GET_COMBINED_ROW_HASH
void messaging_service::register_repair_get_combined_row_hash(std::function<future<get_combined_row_hash_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_COMBINED_ROW_HASH, std::move(func));
}
future<> messaging_service::unregister_repair_get_combined_row_hash() {
    return unregister_handler(messaging_verb::REPAIR_GET_COMBINED_ROW_HASH);
}
future<get_combined_row_hash_response> messaging_service::send_repair_get_combined_row_hash(msg_addr id, uint32_t repair_meta_id, std::optional<repair_sync_boundary> common_sync_boundary) {
    return send_message<future<get_combined_row_hash_response>>(this, messaging_verb::REPAIR_GET_COMBINED_ROW_HASH, std::move(id), repair_meta_id, std::move(common_sync_boundary));
}

void messaging_service::register_repair_get_sync_boundary(std::function<future<get_sync_boundary_response> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_SYNC_BOUNDARY, std::move(func));
}
future<> messaging_service::unregister_repair_get_sync_boundary() {
    return unregister_handler(messaging_verb::REPAIR_GET_SYNC_BOUNDARY);
}
future<get_sync_boundary_response> messaging_service::send_repair_get_sync_boundary(msg_addr id, uint32_t repair_meta_id, std::optional<repair_sync_boundary> skipped_sync_boundary) {
    return send_message<future<get_sync_boundary_response>>(this, messaging_verb::REPAIR_GET_SYNC_BOUNDARY, std::move(id), repair_meta_id, std::move(skipped_sync_boundary));
}

// Wrapper for REPAIR_GET_ROW_DIFF
void messaging_service::register_repair_get_row_diff(std::function<future<repair_rows_on_wire> (const rpc::client_info& cinfo, uint32_t repair_meta_id, std::unordered_set<repair_hash> set_diff, bool needs_all_rows)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_ROW_DIFF, std::move(func));
}
future<> messaging_service::unregister_repair_get_row_diff() {
    return unregister_handler(messaging_verb::REPAIR_GET_ROW_DIFF);
}
future<repair_rows_on_wire> messaging_service::send_repair_get_row_diff(msg_addr id, uint32_t repair_meta_id, std::unordered_set<repair_hash> set_diff, bool needs_all_rows) {
    return send_message<future<repair_rows_on_wire>>(this, messaging_verb::REPAIR_GET_ROW_DIFF, std::move(id), repair_meta_id, std::move(set_diff), needs_all_rows);
}

// Wrapper for REPAIR_PUT_ROW_DIFF
void messaging_service::register_repair_put_row_diff(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, repair_rows_on_wire row_diff)>&& func) {
    register_handler(this, messaging_verb::REPAIR_PUT_ROW_DIFF, std::move(func));
}
future<> messaging_service::unregister_repair_put_row_diff() {
    return unregister_handler(messaging_verb::REPAIR_PUT_ROW_DIFF);
}
future<> messaging_service::send_repair_put_row_diff(msg_addr id, uint32_t repair_meta_id, repair_rows_on_wire row_diff) {
    return send_message<void>(this, messaging_verb::REPAIR_PUT_ROW_DIFF, std::move(id), repair_meta_id, std::move(row_diff));
}

// Wrapper for REPAIR_ROW_LEVEL_START
void messaging_service::register_repair_row_level_start(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, rpc::optional<streaming::stream_reason> reason)>&& func) {
    register_handler(this, messaging_verb::REPAIR_ROW_LEVEL_START, std::move(func));
}
future<> messaging_service::unregister_repair_row_level_start() {
    return unregister_handler(messaging_verb::REPAIR_ROW_LEVEL_START);
}
future<> messaging_service::send_repair_row_level_start(msg_addr id, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range, row_level_diff_detect_algorithm algo, uint64_t max_row_buf_size, uint64_t seed, unsigned remote_shard, unsigned remote_shard_count, unsigned remote_ignore_msb, sstring remote_partitioner_name, table_schema_version schema_version, streaming::stream_reason reason) {
    return send_message<void>(this, messaging_verb::REPAIR_ROW_LEVEL_START, std::move(id), repair_meta_id, std::move(keyspace_name), std::move(cf_name), std::move(range), algo, max_row_buf_size, seed, remote_shard, remote_shard_count, remote_ignore_msb, std::move(remote_partitioner_name), std::move(schema_version), reason);
}

// Wrapper for REPAIR_ROW_LEVEL_STOP
void messaging_service::register_repair_row_level_stop(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range)>&& func) {
    register_handler(this, messaging_verb::REPAIR_ROW_LEVEL_STOP, std::move(func));
}
future<> messaging_service::unregister_repair_row_level_stop() {
    return unregister_handler(messaging_verb::REPAIR_ROW_LEVEL_STOP);
}
future<> messaging_service::send_repair_row_level_stop(msg_addr id, uint32_t repair_meta_id, sstring keyspace_name, sstring cf_name, dht::token_range range) {
    return send_message<void>(this, messaging_verb::REPAIR_ROW_LEVEL_STOP, std::move(id), repair_meta_id, std::move(keyspace_name), std::move(cf_name), std::move(range));
}

// Wrapper for REPAIR_GET_ESTIMATED_PARTITIONS
void messaging_service::register_repair_get_estimated_partitions(std::function<future<uint64_t> (const rpc::client_info& cinfo, uint32_t repair_meta_id)>&& func) {
    register_handler(this, messaging_verb::REPAIR_GET_ESTIMATED_PARTITIONS, std::move(func));
}
future<> messaging_service::unregister_repair_get_estimated_partitions() {
    return unregister_handler(messaging_verb::REPAIR_GET_ESTIMATED_PARTITIONS);
}
future<uint64_t> messaging_service::send_repair_get_estimated_partitions(msg_addr id, uint32_t repair_meta_id) {
    return send_message<future<uint64_t>>(this, messaging_verb::REPAIR_GET_ESTIMATED_PARTITIONS, std::move(id), repair_meta_id);
}

// Wrapper for REPAIR_SET_ESTIMATED_PARTITIONS
void messaging_service::register_repair_set_estimated_partitions(std::function<future<> (const rpc::client_info& cinfo, uint32_t repair_meta_id, uint64_t estimated_partitions)>&& func) {
    register_handler(this, messaging_verb::REPAIR_SET_ESTIMATED_PARTITIONS, std::move(func));
}
future<> messaging_service::unregister_repair_set_estimated_partitions() {
    return unregister_handler(messaging_verb::REPAIR_SET_ESTIMATED_PARTITIONS);
}
future<> messaging_service::send_repair_set_estimated_partitions(msg_addr id, uint32_t repair_meta_id, uint64_t estimated_partitions) {
    return send_message<void>(this, messaging_verb::REPAIR_SET_ESTIMATED_PARTITIONS, std::move(id), repair_meta_id, estimated_partitions);
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

void
messaging_service::register_paxos_prepare(std::function<future<foreign_ptr<std::unique_ptr<service::paxos::prepare_response>>>(
        const rpc::client_info&, rpc::opt_time_point, query::read_command cmd, partition_key key, utils::UUID ballot,
        bool only_digest, query::digest_algorithm da, std::optional<tracing::trace_info>)>&& func) {
    register_handler(this, messaging_verb::PAXOS_PREPARE, std::move(func));
}
future<> messaging_service::unregister_paxos_prepare() {
    return unregister_handler(netw::messaging_verb::PAXOS_PREPARE);
}
future<service::paxos::prepare_response>
messaging_service::send_paxos_prepare(gms::inet_address peer, clock_type::time_point timeout,
        const query::read_command& cmd, const partition_key& key, utils::UUID ballot,
        bool only_digest, query::digest_algorithm da, std::optional<tracing::trace_info> trace_info) {
    return send_message_timeout<service::paxos::prepare_response>(this,
        messaging_verb::PAXOS_PREPARE, netw::msg_addr(peer), timeout, cmd, key, ballot, only_digest, da, std::move(trace_info));
}

void messaging_service::register_paxos_accept(std::function<future<bool>(
        const rpc::client_info&, rpc::opt_time_point, service::paxos::proposal proposal,
        std::optional<tracing::trace_info>)>&& func) {
    register_handler(this, messaging_verb::PAXOS_ACCEPT, std::move(func));
}
future<> messaging_service::unregister_paxos_accept() {
    return unregister_handler(netw::messaging_verb::PAXOS_ACCEPT);
}
future<bool>
messaging_service::send_paxos_accept(gms::inet_address peer, clock_type::time_point timeout,
        const service::paxos::proposal& proposal, std::optional<tracing::trace_info> trace_info) {

    return send_message_timeout<future<bool>>(this,
        messaging_verb::PAXOS_ACCEPT, netw::msg_addr(peer), timeout, proposal, std::move(trace_info));
}

void messaging_service::register_paxos_learn(std::function<future<rpc::no_wait_type> (const rpc::client_info&,
    rpc::opt_time_point, service::paxos::proposal decision, std::vector<inet_address> forward, inet_address reply_to,
    unsigned shard, response_id_type response_id, std::optional<tracing::trace_info> trace_info)>&& func) {
    register_handler(this, netw::messaging_verb::PAXOS_LEARN, std::move(func));
}
future<> messaging_service::unregister_paxos_learn() {
    return unregister_handler(netw::messaging_verb::PAXOS_LEARN);
}
future<> messaging_service::send_paxos_learn(msg_addr id, clock_type::time_point timeout, const service::paxos::proposal& decision,
    std::vector<inet_address> forward, inet_address reply_to, unsigned shard, response_id_type response_id,
    std::optional<tracing::trace_info> trace_info) {
    return send_message_oneway_timeout(this, timeout, messaging_verb::PAXOS_LEARN, std::move(id), decision, std::move(forward),
        std::move(reply_to), shard, std::move(response_id), std::move(trace_info));
}

void messaging_service::register_paxos_prune(std::function<future<rpc::no_wait_type>(
        const rpc::client_info&, rpc::opt_time_point, UUID schema_id, partition_key key, utils::UUID ballot, std::optional<tracing::trace_info>)>&& func) {
    register_handler(this, messaging_verb::PAXOS_PRUNE, std::move(func));
}
future<> messaging_service::unregister_paxos_prune() {
    return unregister_handler(netw::messaging_verb::PAXOS_PRUNE);
}
future<>
messaging_service::send_paxos_prune(gms::inet_address peer, clock_type::time_point timeout, UUID schema_id,
        const partition_key& key, utils::UUID ballot, std::optional<tracing::trace_info> trace_info) {
    return send_message_oneway_timeout(this, timeout, messaging_verb::PAXOS_PRUNE, netw::msg_addr(peer), schema_id, key, ballot, std::move(trace_info));
}

void messaging_service::register_hint_mutation(std::function<future<rpc::no_wait_type> (const rpc::client_info&, rpc::opt_time_point, frozen_mutation fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id, rpc::optional<std::optional<tracing::trace_info>> trace_info)>&& func) {
    register_handler(this, netw::messaging_verb::HINT_MUTATION, std::move(func));
}
future<> messaging_service::unregister_hint_mutation() {
    return unregister_handler(netw::messaging_verb::HINT_MUTATION);
}
future<> messaging_service::send_hint_mutation(msg_addr id, clock_type::time_point timeout, const frozen_mutation& fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id, std::optional<tracing::trace_info> trace_info) {
    return send_message_oneway_timeout(this, timeout, messaging_verb::HINT_MUTATION, std::move(id), fm, std::move(forward),
        std::move(reply_to), shard, std::move(response_id), std::move(trace_info));
}

} // namespace net
