/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
#include "core/distributed.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"
#include "streaming/prepare_message.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "rpc/rpc.hh"
#include "db/config.hh"
#include "dht/i_partitioner.hh"
#include "range.hh"
#include "frozen_schema.hh"
#include "serializer_impl.hh"

namespace net {

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

// For vectors
template <typename T, typename Output>
void write(serializer, Output& out, const std::vector<T>& data) {
    ser::serialize(out, data);
}
template <typename T, typename Input>
std::vector<T> read(serializer, Input& in, boost::type<std::vector<T>> type) {
    return ser::deserialize(in, type);
}

// Gossip syn
template<typename Output>
void write(serializer, Output& out, const gms::gossip_digest_syn& data) {
    ser::serialize(out, data);
}

template <typename Input>
gms::gossip_digest_syn
read(serializer, Input& in, boost::type<gms::gossip_digest_syn> type) {
    return ser::deserialize(in, type);
}

// Gossip ack
template<typename Output>
void write(serializer, Output& out, const gms::gossip_digest_ack& data) {
    ser::serialize(out, data);
}

template <typename Input>
gms::gossip_digest_ack
read(serializer, Input& in, boost::type<gms::gossip_digest_ack> type) {
    return ser::deserialize(in, type);
}

// Gossip ack2
template<typename Output>
void write(serializer, Output& out, const gms::gossip_digest_ack2& data) {
    ser::serialize(out, data);
}

template <typename Input>
gms::gossip_digest_ack2
read(serializer, Input& in, boost::type<gms::gossip_digest_ack2> type) {
    return ser::deserialize(in, type);
}

// Gossip digest
template<typename Output>
void write(serializer, Output& out, const gms::gossip_digest& data) {
    ser::serialize(out, data);
}

template <typename Input>
gms::gossip_digest
read(serializer, Input& in, boost::type<gms::gossip_digest> type) {
    return ser::deserialize(in, type);
}

// Gossip versioned_value
template<typename Output>
void write(serializer, Output& out, const gms::versioned_value& data) {
    ser::serialize(out, data);
}

template <typename Input>
gms::versioned_value
read(serializer, Input& in, boost::type<gms::versioned_value> type) {
    return ser::deserialize(in, type);
}

// Gossip endpoint_state
template<typename Output>
void write(serializer, Output& out, const gms::endpoint_state& data) {
    ser::serialize(out, data);
}

template <typename Input>
gms::endpoint_state
read(serializer, Input& in, boost::type<gms::endpoint_state> type) {
    return ser::deserialize(in, type);
}

// Gossip heart_beat_state
template<typename Output>
void write(serializer, Output& out, const gms::heart_beat_state& data) {
    ser::serialize(out, data);
}

template <typename Input>
gms::heart_beat_state
read(serializer, Input& in, boost::type<gms::heart_beat_state> type) {
    return ser::deserialize(in, type);
}

template<typename Output>
void write(serializer, Output& out, const query::read_command& data) {
    ser::serialize(out, data);
}

template <typename Input>
query::read_command
read(serializer, Input& in, boost::type<query::read_command> type) {
    return ser::deserialize(in, type);
}

template<typename Output>
void write(serializer, Output& out, const query::partition_range& data) {
    ser::serialize(out, data);
}

template <typename Input>
query::partition_range
read(serializer, Input& in, boost::type<query::partition_range> type) {
    return ser::deserialize(in, type);
}

template<typename Output>
void write(serializer, Output& out, const query::result& data) {
    ser::serialize(out, data);
}

template <typename Input>
query::result
read(serializer, Input& in, boost::type<query::result> type) {
    return ser::deserialize(in, type);
}

template<typename Output>
void write(serializer, Output& out, const frozen_mutation& data) {
    ser::serialize(out, data);
}

template <typename Input>
frozen_mutation
read(serializer, Input& in, boost::type<frozen_mutation> type) {
    return ser::deserialize(in, type);
}

template<typename Output>
void write(serializer, Output& out, const reconcilable_result& data) {
    ser::serialize(out, data);
}

template <typename Input>
reconcilable_result
read(serializer, Input& in, boost::type<reconcilable_result> type) {
    return ser::deserialize(in, type);
}

// streaming reqeust
template<typename Output>
void write(serializer, Output& out, const streaming::stream_request& data) {
    ser::serialize(out, data);
}

template <typename Input>
streaming::stream_request
read(serializer, Input& in, boost::type<streaming::stream_request> type) {
    return ser::deserialize(in, type);
}

// streaming summary
template<typename Output>
void write(serializer, Output& out, const streaming::stream_summary& data) {
    ser::serialize(out, data);
}

template <typename Input>
streaming::stream_summary
read(serializer, Input& in, boost::type<streaming::stream_request> type) {
    return ser::deserialize(in, type);
}

// streaming prepare_message
template<typename Output>
void write(serializer, Output& out, const streaming::prepare_message& data) {
    ser::serialize(out, data);
}

template <typename Input>
streaming::prepare_message
read(serializer, Input& in, boost::type<streaming::prepare_message> type) {
    return ser::deserialize(in, type);
}

static logging::logger logger("messaging_service");
static logging::logger rpc_logger("rpc");

using inet_address = gms::inet_address;
using gossip_digest_syn = gms::gossip_digest_syn;
using gossip_digest_ack = gms::gossip_digest_ack;
using gossip_digest_ack2 = gms::gossip_digest_ack2;
using rpc_protocol = rpc::protocol<serializer, messaging_verb>;
using namespace std::chrono_literals;
template <typename Output>
void net::serializer::write(Output& out, const query::result& v) const {
    write_serializable(out, v);
}
template <typename Input>
query::result net::serializer::read(Input& in, boost::type<query::result>) const {
    return read_serializable<query::result>(in);
}


struct messaging_service::rpc_protocol_wrapper : public rpc_protocol { using rpc_protocol::rpc_protocol; };

// This wrapper pretends to be rpc_protocol::client, but also handles
// stopping it before destruction, in case it wasn't stopped already.
// This should be integrated into messaging_service proper.
class messaging_service::rpc_protocol_client_wrapper {
    std::unique_ptr<rpc_protocol::client> _p;
public:
    rpc_protocol_client_wrapper(rpc_protocol& proto, ipv4_addr addr, ipv4_addr local = ipv4_addr())
            : _p(std::make_unique<rpc_protocol::client>(proto, addr, local)) {
    }
    rpc_protocol_client_wrapper(rpc_protocol& proto, ipv4_addr addr, ipv4_addr local, ::shared_ptr<seastar::tls::server_credentials> c)
            : _p(std::make_unique<rpc_protocol::client>(proto, addr, seastar::tls::connect(c, addr, local)))
    {}
    auto get_stats() const { return _p->get_stats(); }
    future<> stop() { return _p->stop(); }
    bool error() {
        return _p->error();
    }
    operator rpc_protocol::client&() { return *_p; }
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
    return std::hash<uint32_t>()(id.addr.raw_addr());
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
    _server->foreach_connection([f](const rpc_protocol::server::connection& c) {
        f(c.info(), c.get_stats());
    });
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

// Register a handler (a callback lambda) for verb
template <typename Func>
void register_handler(messaging_service* ms, messaging_verb verb, Func&& func) {
    ms->rpc()->register_handler(verb, std::move(func));
}

messaging_service::messaging_service(gms::inet_address ip, uint16_t port)
    : messaging_service(std::move(ip), port, encrypt_what::none, 0, nullptr)
{}

static
rpc::resource_limits
rpc_resource_limits() {
    rpc::resource_limits limits;
    limits.bloat_factor = 3;
    limits.basic_request_size = 1000;
    limits.max_memory = std::max<size_t>(0.08 * memory::stats().total_memory(), 1'000'000);
    return limits;
}

messaging_service::messaging_service(gms::inet_address ip
        , uint16_t port
        , encrypt_what ew
        , uint16_t ssl_port
        , ::shared_ptr<seastar::tls::server_credentials> credentials
        )
    : _listen_address(ip)
    , _port(port)
    , _ssl_port(ssl_port)
    , _encrypt_what(ew)
    , _rpc(new rpc_protocol_wrapper(serializer { }))
    , _server(new rpc_protocol_server_wrapper(*_rpc, ipv4_addr { _listen_address.raw_addr(), _port }, rpc_resource_limits()))
    , _credentials(std::move(credentials))
    , _server_tls([this]() -> std::unique_ptr<rpc_protocol_server_wrapper>{
        if (_encrypt_what == encrypt_what::none) {
            return nullptr;
        }
        listen_options lo;
        lo.reuse_address = true;
        return std::make_unique<rpc_protocol_server_wrapper>(*_rpc,
                        seastar::tls::listen(_credentials
                                        , make_ipv4_address(ipv4_addr {_listen_address.raw_addr(), _ssl_port})
                                        , lo)
        );
    }())
{
    _rpc->set_logger([] (const sstring& log) {
            rpc_logger.info("{}", log);
    });
    register_handler(this, messaging_verb::CLIENT_ID, [] (rpc::client_info& ci, gms::inet_address broadcast_address, uint32_t src_cpu_id) {
        ci.attach_auxiliary("baddr", broadcast_address);
        ci.attach_auxiliary("src_cpu_id", src_cpu_id);
        return rpc::no_wait;
    });
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

future<> messaging_service::stop() {
    return when_all(
        _server->stop(),
        parallel_for_each(_clients, [] (auto& m) {
            return parallel_for_each(m, [] (std::pair<const msg_addr, shard_info>& c) {
                return c.second.rpc_client->stop();
            });
        })
    ).discard_result();
}

rpc::no_wait_type messaging_service::no_wait() {
    return rpc::no_wait;
}

static unsigned get_rpc_client_idx(messaging_verb verb) {
    unsigned idx = 0;
    // GET_SCHEMA_VERSION is sent from read/mutate verbs so should be
    // sent on a different connection to avoid potential deadlocks
    // as well as reduce latency as there are potentially many requests
    // blocked on schema version request.
    if (verb == messaging_verb::GOSSIP_DIGEST_SYN ||
        verb == messaging_verb::GOSSIP_DIGEST_ACK2 ||
        verb == messaging_verb::GOSSIP_SHUTDOWN ||
        verb == messaging_verb::GOSSIP_ECHO ||
        verb == messaging_verb::GET_SCHEMA_VERSION) {
        idx = 1;
    }
    return idx;
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
            msg_addr id = {
                .addr = p.first
            };

            this->remove_rpc_client(id);
        }
    });
}

void messaging_service::cache_preferred_ip(gms::inet_address ep, gms::inet_address ip) {
    _preferred_ip_cache[ep] = ip;
}

shared_ptr<messaging_service::rpc_protocol_client_wrapper> messaging_service::get_rpc_client(messaging_verb verb, msg_addr id) {
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

    auto remote_addr = ipv4_addr(get_preferred_ip(id.addr).raw_addr(), must_encrypt ? _ssl_port : _port);
    auto local_addr = ipv4_addr{_listen_address.raw_addr(), 0};

    auto client = must_encrypt ?
                    ::make_shared<rpc_protocol_client_wrapper>(*_rpc,
                                    remote_addr, local_addr, _credentials) :
                    ::make_shared<rpc_protocol_client_wrapper>(*_rpc,
                                    remote_addr, local_addr);

    it = _clients[idx].emplace(id, shard_info(std::move(client))).first;
    uint32_t src_cpu_id = engine().cpu_id();
    _rpc->make_client<rpc::no_wait_type(gms::inet_address, uint32_t)>(messaging_verb::CLIENT_ID)(*it->second.rpc_client, utils::fb_utilities::get_broadcast_address(), src_cpu_id);
    return it->second.rpc_client;
}

void messaging_service::remove_rpc_client_one(clients_map& clients, msg_addr id, bool dead_only) {
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
        client->stop().finally([id, client, ms = shared_from_this()] {
            logger.debug("dropped connection to {}", id.addr);
        }).discard_result();
    }
}

void messaging_service::remove_error_rpc_client(messaging_verb verb, msg_addr id) {
    remove_rpc_client_one(_clients[get_rpc_client_idx(verb)], id, true);
}

void messaging_service::remove_rpc_client(msg_addr id) {
    for (auto& c : _clients) {
        remove_rpc_client_one(c, id, false);
    }
}

std::unique_ptr<messaging_service::rpc_protocol_wrapper>& messaging_service::rpc() {
    return _rpc;
}

// Send a message for verb
template <typename MsgIn, typename... MsgOut>
auto send_message(messaging_service* ms, messaging_verb verb, msg_addr id, MsgOut&&... msg) {
    auto rpc_client_ptr = ms->get_rpc_client(verb, id);
    auto rpc_handler = ms->rpc()->make_client<MsgIn(MsgOut...)>(verb);
    auto& rpc_client = *rpc_client_ptr;
    return rpc_handler(rpc_client, std::forward<MsgOut>(msg)...).then_wrapped([ms = ms->shared_from_this(), id, verb, rpc_client_ptr = std::move(rpc_client_ptr)] (auto&& f) {
        try {
            if (f.failed()) {
                ms->increment_dropped_messages(verb);
                f.get();
                assert(false); // never reached
            }
            return std::move(f);
        } catch (rpc::closed_error) {
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
    auto rpc_client_ptr = ms->get_rpc_client(verb, id);
    auto rpc_handler = ms->rpc()->make_client<MsgIn(MsgOut...)>(verb);
    auto& rpc_client = *rpc_client_ptr;
    return rpc_handler(rpc_client, timeout, std::forward<MsgOut>(msg)...).then_wrapped([ms = ms->shared_from_this(), id, verb, rpc_client_ptr = std::move(rpc_client_ptr)] (auto&& f) {
        try {
            if (f.failed()) {
                ms->increment_dropped_messages(verb);
                f.get();
                assert(false); // never reached
            }
            return std::move(f);
        } catch (rpc::closed_error) {
            // This is a transport error
            ms->remove_error_rpc_client(verb, id);
            throw;
        } catch (...) {
            // This is expected to be a rpc server error, e.g., the rpc handler throws a std::runtime_error.
            throw;
        }
    });
}

template <typename MsgIn, typename... MsgOut>
auto send_message_timeout_and_retry(messaging_service* ms, messaging_verb verb, msg_addr id,
        std::chrono::seconds timeout, int nr_retry, std::chrono::seconds wait, MsgOut... msg) {
    namespace stdx = std::experimental;
    using MsgInTuple = typename futurize_t<MsgIn>::value_type;
    return do_with(int(nr_retry), std::move(msg)..., [ms, verb, id, timeout, wait, nr_retry] (auto& retry, const auto&... messages) {
        return repeat_until_value([ms, verb, id, timeout, wait, nr_retry, &retry, &messages...] {
            return send_message_timeout<MsgIn>(ms, verb, id, timeout, messages...).then_wrapped(
                    [verb, id, timeout, wait, nr_retry, &retry] (auto&& f) mutable {
                try {
                    MsgInTuple ret = f.get();
                    if (retry != nr_retry) {
                        logger.info("Retry verb={} to {}, retry={}: OK", int(verb), id, retry);
                    }
                    return make_ready_future<stdx::optional<MsgInTuple>>(std::move(ret));
                } catch (rpc::timeout_error) {
                    logger.info("Retry verb={} to {}, retry={}: timeout in {} seconds", int(verb), id, retry, timeout.count());
                    throw;
                } catch (rpc::closed_error) {
                    logger.info("Retry verb={} to {}, retry={}: {}", int(verb), id, retry, std::current_exception());
                    if (--retry == 0) {
                        throw;
                    }
                    return sleep(wait).then([] {
                        return make_ready_future<stdx::optional<MsgInTuple>>(stdx::nullopt);
                    });
                } catch (...) {
                    throw;
                }
            });
        }).then([] (MsgInTuple result) {
            return futurize<MsgIn>::from_tuple(std::move(result));
        });
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

// Retransmission parameters for streaming verbs.
// A stream plan gives up retrying in 10*30 + 10*60 seconds (15 minutes) at
// most, 10*30 seconds (5 minutes) at least.
static constexpr int streaming_nr_retry = 10;
static constexpr std::chrono::seconds streaming_timeout{10*60};
static constexpr std::chrono::seconds streaming_wait_before_retry{30};

// PREPARE_MESSAGE
void messaging_service::register_prepare_message(std::function<future<streaming::prepare_message> (const rpc::client_info& cinfo,
        streaming::prepare_message msg, UUID plan_id, sstring description)>&& func) {
    register_handler(this, messaging_verb::PREPARE_MESSAGE, std::move(func));
}
future<streaming::prepare_message> messaging_service::send_prepare_message(msg_addr id, streaming::prepare_message msg, UUID plan_id,
        sstring description) {
    return send_message_timeout_and_retry<streaming::prepare_message>(this, messaging_verb::PREPARE_MESSAGE, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        std::move(msg), plan_id, std::move(description));
}

// PREPARE_DONE_MESSAGE
void messaging_service::register_prepare_done_message(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::PREPARE_DONE_MESSAGE, std::move(func));
}
future<> messaging_service::send_prepare_done_message(msg_addr id, UUID plan_id, unsigned dst_cpu_id) {
    return send_message_timeout_and_retry<void>(this, messaging_verb::PREPARE_DONE_MESSAGE, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        plan_id, dst_cpu_id);
}

// STREAM_MUTATION
void messaging_service::register_stream_mutation(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION, std::move(func));
}
future<> messaging_service::send_stream_mutation(msg_addr id, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id) {
    return send_message_timeout_and_retry<void>(this, messaging_verb::STREAM_MUTATION, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        plan_id, std::move(fm), dst_cpu_id);
}

// STREAM_MUTATION_DONE
void messaging_service::register_stream_mutation_done(std::function<future<> (const rpc::client_info& cinfo,
        UUID plan_id, std::vector<range<dht::token>> ranges, UUID cf_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION_DONE, std::move(func));
}
future<> messaging_service::send_stream_mutation_done(msg_addr id, UUID plan_id, std::vector<range<dht::token>> ranges, UUID cf_id, unsigned dst_cpu_id) {
    return send_message_timeout_and_retry<void>(this, messaging_verb::STREAM_MUTATION_DONE, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        plan_id, std::move(ranges), cf_id, dst_cpu_id);
}

// COMPLETE_MESSAGE
void messaging_service::register_complete_message(std::function<future<> (const rpc::client_info& cinfo, UUID plan_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::COMPLETE_MESSAGE, std::move(func));
}
future<> messaging_service::send_complete_message(msg_addr id, UUID plan_id, unsigned dst_cpu_id) {
    return send_message_timeout_and_retry<void>(this, messaging_verb::COMPLETE_MESSAGE, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        plan_id, dst_cpu_id);
}

void messaging_service::register_gossip_echo(std::function<future<> ()>&& func) {
    register_handler(this, messaging_verb::GOSSIP_ECHO, std::move(func));
}
void messaging_service::unregister_gossip_echo() {
    _rpc->unregister_handler(net::messaging_verb::GOSSIP_ECHO);
}
future<> messaging_service::send_gossip_echo(msg_addr id) {
    return send_message_timeout<void>(this, messaging_verb::GOSSIP_ECHO, std::move(id), 3000ms);
}

void messaging_service::register_gossip_shutdown(std::function<rpc::no_wait_type (inet_address from)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_SHUTDOWN, std::move(func));
}
void messaging_service::unregister_gossip_shutdown() {
    _rpc->unregister_handler(net::messaging_verb::GOSSIP_SHUTDOWN);
}
future<> messaging_service::send_gossip_shutdown(msg_addr id, inet_address from) {
    return send_message_oneway(this, messaging_verb::GOSSIP_SHUTDOWN, std::move(id), std::move(from));
}

void messaging_service::register_gossip_digest_syn(std::function<future<gossip_digest_ack> (gossip_digest_syn)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_SYN, std::move(func));
}
void messaging_service::unregister_gossip_digest_syn() {
    _rpc->unregister_handler(net::messaging_verb::GOSSIP_DIGEST_SYN);
}
future<gossip_digest_ack> messaging_service::send_gossip_digest_syn(msg_addr id, gossip_digest_syn msg) {
    return send_message_timeout<gossip_digest_ack>(this, messaging_verb::GOSSIP_DIGEST_SYN, std::move(id), 3000ms, std::move(msg));
}

void messaging_service::register_gossip_digest_ack2(std::function<rpc::no_wait_type (gossip_digest_ack2)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_ACK2, std::move(func));
}
void messaging_service::unregister_gossip_digest_ack2() {
    _rpc->unregister_handler(net::messaging_verb::GOSSIP_DIGEST_ACK2);
}
future<> messaging_service::send_gossip_digest_ack2(msg_addr id, gossip_digest_ack2 msg) {
    return send_message_oneway(this, messaging_verb::GOSSIP_DIGEST_ACK2, std::move(id), std::move(msg));
}

void messaging_service::register_definitions_update(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, std::vector<frozen_mutation> fm)>&& func) {
    register_handler(this, net::messaging_verb::DEFINITIONS_UPDATE, std::move(func));
}
void messaging_service::unregister_definitions_update() {
    _rpc->unregister_handler(net::messaging_verb::DEFINITIONS_UPDATE);
}
future<> messaging_service::send_definitions_update(msg_addr id, std::vector<frozen_mutation> fm) {
    return send_message_oneway(this, messaging_verb::DEFINITIONS_UPDATE, std::move(id), std::move(fm));
}

void messaging_service::register_migration_request(std::function<future<std::vector<frozen_mutation>> ()>&& func) {
    register_handler(this, net::messaging_verb::MIGRATION_REQUEST, std::move(func));
}
void messaging_service::unregister_migration_request() {
    _rpc->unregister_handler(net::messaging_verb::MIGRATION_REQUEST);
}
future<std::vector<frozen_mutation>> messaging_service::send_migration_request(msg_addr id) {
    return send_message<std::vector<frozen_mutation>>(this, messaging_verb::MIGRATION_REQUEST, std::move(id));
}

void messaging_service::register_mutation(std::function<future<rpc::no_wait_type> (const rpc::client_info&, frozen_mutation fm, std::vector<inet_address> forward,
    inet_address reply_to, unsigned shard, response_id_type response_id)>&& func) {
    register_handler(this, net::messaging_verb::MUTATION, std::move(func));
}
void messaging_service::unregister_mutation() {
    _rpc->unregister_handler(net::messaging_verb::MUTATION);
}
future<> messaging_service::send_mutation(msg_addr id, clock_type::time_point timeout, const frozen_mutation& fm, std::vector<inet_address> forward,
    inet_address reply_to, unsigned shard, response_id_type response_id) {
    return send_message_oneway_timeout(this, timeout, messaging_verb::MUTATION, std::move(id), fm, std::move(forward),
        std::move(reply_to), std::move(shard), std::move(response_id));
}

void messaging_service::register_mutation_done(std::function<future<rpc::no_wait_type> (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id)>&& func) {
    register_handler(this, net::messaging_verb::MUTATION_DONE, std::move(func));
}
void messaging_service::unregister_mutation_done() {
    _rpc->unregister_handler(net::messaging_verb::MUTATION_DONE);
}
future<> messaging_service::send_mutation_done(msg_addr id, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::MUTATION_DONE, std::move(id), std::move(shard), std::move(response_id));
}

void messaging_service::register_read_data(std::function<future<foreign_ptr<lw_shared_ptr<query::result>>> (const rpc::client_info&, query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_DATA, std::move(func));
}
void messaging_service::unregister_read_data() {
    _rpc->unregister_handler(net::messaging_verb::READ_DATA);
}
future<query::result> messaging_service::send_read_data(msg_addr id, const query::read_command& cmd, const query::partition_range& pr) {
    return send_message<query::result>(this, messaging_verb::READ_DATA, std::move(id), cmd, pr);
}

void messaging_service::register_get_schema_version(std::function<future<frozen_schema>(unsigned, table_schema_version)>&& func) {
    register_handler(this, net::messaging_verb::GET_SCHEMA_VERSION, std::move(func));
}
void messaging_service::unregister_get_schema_version() {
    _rpc->unregister_handler(net::messaging_verb::GET_SCHEMA_VERSION);
}
future<frozen_schema> messaging_service::send_get_schema_version(msg_addr dst, table_schema_version v) {
    return send_message<frozen_schema>(this, messaging_verb::GET_SCHEMA_VERSION, dst, static_cast<unsigned>(dst.cpu_id), v);
}

void messaging_service::register_read_mutation_data(std::function<future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> (const rpc::client_info&, query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_MUTATION_DATA, std::move(func));
}
void messaging_service::unregister_read_mutation_data() {
    _rpc->unregister_handler(net::messaging_verb::READ_MUTATION_DATA);
}
future<reconcilable_result> messaging_service::send_read_mutation_data(msg_addr id, const query::read_command& cmd, const query::partition_range& pr) {
    return send_message<reconcilable_result>(this, messaging_verb::READ_MUTATION_DATA, std::move(id), cmd, pr);
}

void messaging_service::register_read_digest(std::function<future<query::result_digest> (const rpc::client_info&, query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_DIGEST, std::move(func));
}
void messaging_service::unregister_read_digest() {
    _rpc->unregister_handler(net::messaging_verb::READ_DIGEST);
}
future<query::result_digest> messaging_service::send_read_digest(msg_addr id, const query::read_command& cmd, const query::partition_range& pr) {
    return send_message<query::result_digest>(this, net::messaging_verb::READ_DIGEST, std::move(id), cmd, pr);
}

// Wrapper for TRUNCATE
void messaging_service::register_truncate(std::function<future<> (sstring, sstring)>&& func) {
    register_handler(this, net::messaging_verb::TRUNCATE, std::move(func));
}

void messaging_service::unregister_truncate() {
    _rpc->unregister_handler(net::messaging_verb::TRUNCATE);
}

future<> messaging_service::send_truncate(msg_addr id, std::chrono::milliseconds timeout, sstring ks, sstring cf) {
    return send_message_timeout<void>(this, net::messaging_verb::TRUNCATE, std::move(id), std::move(timeout), std::move(ks), std::move(cf));
}

// Wrapper for REPLICATION_FINISHED
void messaging_service::register_replication_finished(std::function<future<> (inet_address)>&& func) {
    register_handler(this, messaging_verb::REPLICATION_FINISHED, std::move(func));
}
void messaging_service::unregister_replication_finished() {
    _rpc->unregister_handler(messaging_verb::REPLICATION_FINISHED);
}
future<> messaging_service::send_replication_finished(msg_addr id, inet_address from) {
    // FIXME: getRpcTimeout : conf.request_timeout_in_ms
    return send_message_timeout<void>(this, messaging_verb::REPLICATION_FINISHED, std::move(id), 10000ms, std::move(from));
}

// Wrapper for REPAIR_CHECKSUM_RANGE
void messaging_service::register_repair_checksum_range(
        std::function<future<partition_checksum> (sstring keyspace,
                sstring cf, query::range<dht::token> range)>&& f) {
    register_handler(this, messaging_verb::REPAIR_CHECKSUM_RANGE, std::move(f));
}
void messaging_service::unregister_repair_checksum_range() {
    _rpc->unregister_handler(messaging_verb::REPAIR_CHECKSUM_RANGE);
}
future<partition_checksum> messaging_service::send_repair_checksum_range(
        msg_addr id, sstring keyspace, sstring cf, ::range<dht::token> range)
{
    return send_message<partition_checksum>(this,
            messaging_verb::REPAIR_CHECKSUM_RANGE, std::move(id),
            std::move(keyspace), std::move(cf), std::move(range));
}

} // namespace net
