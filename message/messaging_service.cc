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
#include "streaming/messages/stream_init_message.hh"
#include "streaming/messages/prepare_message.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "rpc/rpc.hh"
#include "db/config.hh"

namespace net {

static logging::logger logger("messaging_service");

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
query::result net::serializer::read(Input& in, rpc::type<query::result>) const {
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

bool operator==(const shard_id& x, const shard_id& y) {
    // Ignore cpu id for now since we do not really support shard to shard connections
    return x.addr == y.addr;
}

bool operator<(const shard_id& x, const shard_id& y) {
    // Ignore cpu id for now since we do not really support shard to shard connections
    if (x.addr < y.addr) {
        return true;
    } else {
        return false;
    }
}

std::ostream& operator<<(std::ostream& os, const shard_id& x) {
    return os << x.addr << ":" << x.cpu_id;
}

size_t shard_id::hash::operator()(const shard_id& id) const {
    // Ignore cpu id for now since we do not really support // shard to shard connections
    return std::hash<uint32_t>()(id.addr.raw_addr());
}

messaging_service::shard_info::shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client)
    : rpc_client(std::move(client)) {
}

rpc::stats messaging_service::shard_info::get_stats() const {
    return rpc_client->get_stats();
}

void messaging_service::foreach_client(std::function<void(const shard_id& id, const shard_info& info)> f) const {
    for (unsigned idx = 0; idx < 2; idx ++) {
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
    : _listen_address(ip)
    , _port(port)
    , _rpc(new rpc_protocol_wrapper(serializer{}))
    , _server(new rpc_protocol_server_wrapper(*_rpc, ipv4_addr{_listen_address.raw_addr(), _port})) {
    register_handler(this, messaging_verb::CLIENT_ID, [] (rpc::client_info& ci, gms::inet_address broadcast_address) {
        ci.attach_auxiliary("baddr", broadcast_address);
        return rpc::no_wait;
    });
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
            return parallel_for_each(m, [] (std::pair<const shard_id, shard_info>& c) {
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
    if (verb == messaging_verb::GOSSIP_DIGEST_SYN ||
        verb == messaging_verb::GOSSIP_DIGEST_ACK ||
        verb == messaging_verb::GOSSIP_DIGEST_ACK2 ||
        verb == messaging_verb::GOSSIP_SHUTDOWN ||
        verb == messaging_verb::ECHO) {
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
            shard_id id = {
                .addr = p.first
            };

            this->remove_rpc_client(id);
        }
    });
}

void messaging_service::cache_preferred_ip(gms::inet_address ep, gms::inet_address ip) {
    _preferred_ip_cache[ep] = ip;
}

shared_ptr<messaging_service::rpc_protocol_client_wrapper> messaging_service::get_rpc_client(messaging_verb verb, shard_id id) {
    auto idx = get_rpc_client_idx(verb);
    auto it = _clients[idx].find(id);

    if (it != _clients[idx].end()) {
        auto c = it->second.rpc_client;
        if (!c->error()) {
            return c;
        }
        remove_error_rpc_client(verb, id);
    }

    auto remote_addr = ipv4_addr(get_preferred_ip(id.addr).raw_addr(), _port);
    auto client = ::make_shared<rpc_protocol_client_wrapper>(*_rpc, remote_addr, ipv4_addr{_listen_address.raw_addr(), 0});
    it = _clients[idx].emplace(id, shard_info(std::move(client))).first;
    _rpc->make_client<rpc::no_wait_type(gms::inet_address)>(messaging_verb::CLIENT_ID)(*it->second.rpc_client, utils::fb_utilities::get_broadcast_address());
    return it->second.rpc_client;
}

void messaging_service::remove_rpc_client_one(clients_map& clients, shard_id id, bool dead_only) {
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

void messaging_service::remove_error_rpc_client(messaging_verb verb, shard_id id) {
    remove_rpc_client_one(_clients[get_rpc_client_idx(verb)], id, true);
}

void messaging_service::remove_rpc_client(shard_id id) {
    for (auto& c : _clients) {
        remove_rpc_client_one(c, id, false);
    }
}

std::unique_ptr<messaging_service::rpc_protocol_wrapper>& messaging_service::rpc() {
    return _rpc;
}

// Send a message for verb
template <typename MsgIn, typename... MsgOut>
auto send_message(messaging_service* ms, messaging_verb verb, shard_id id, MsgOut&&... msg) {
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
template <typename MsgIn, typename... MsgOut>
auto send_message_timeout(messaging_service* ms, messaging_verb verb, shard_id id, std::chrono::milliseconds timeout, MsgOut&&... msg) {
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
auto send_message_timeout_and_retry(messaging_service* ms, messaging_verb verb, shard_id id,
        std::chrono::seconds timeout, int nr_retry, std::chrono::seconds wait, MsgOut... msg) {
    namespace stdx = std::experimental;
    using MsgInTuple = typename futurize_t<MsgIn>::value_type;
    return do_with(int(nr_retry), std::move(msg)..., [ms, verb, id, timeout, wait, nr_retry] (auto& retry, const auto&... messages) {
        return repeat_until_value([ms, verb, id, timeout, wait, nr_retry, &retry, &messages...] {
            return send_message_timeout<MsgIn>(ms, verb, id, timeout, messages...).then_wrapped(
                    [verb, id, wait, nr_retry, &retry] (auto&& f) mutable {
                try {
                    MsgInTuple ret = f.get();
                    if (retry != nr_retry) {
                        logger.info("Retry verb={} to {}, retry={}: OK", int(verb), id, retry);
                    }
                    return make_ready_future<stdx::optional<MsgInTuple>>(std::move(ret));
                } catch (...) {
                    logger.info("Retry verb={} to {}, retry={}: {}", int(verb), id, retry, std::current_exception());
                    if (--retry == 0) {
                        throw;
                    }
                    return sleep(wait).then([] {
                        return make_ready_future<stdx::optional<MsgInTuple>>(stdx::nullopt);
                    });
                }
            });
        }).then([] (MsgInTuple result) {
            return futurize<MsgIn>::from_tuple(std::move(result));
        });
    });
}

// Send one way message for verb
template <typename... MsgOut>
auto send_message_oneway(messaging_service* ms, messaging_verb verb, shard_id id, MsgOut&&... msg) {
    return send_message<rpc::no_wait_type>(ms, std::move(verb), std::move(id), std::forward<MsgOut>(msg)...);
}

// Wrappers for verbs

// Retransmission parameters for streaming verbs
// A stream plan gives up retrying in 5 minutes
static constexpr int streaming_nr_retry = 20;
static constexpr std::chrono::seconds streaming_timeout{10};
static constexpr std::chrono::seconds streaming_wait_before_retry{5};

// STREAM_INIT_MESSAGE
void messaging_service::register_stream_init_message(std::function<future<unsigned> (streaming::messages::stream_init_message msg, unsigned src_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_INIT_MESSAGE, std::move(func));
}
future<unsigned> messaging_service::send_stream_init_message(shard_id id, streaming::messages::stream_init_message msg, unsigned src_cpu_id) {
    return send_message_timeout_and_retry<unsigned>(this, messaging_verb::STREAM_INIT_MESSAGE, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        std::move(msg), src_cpu_id);
}

// PREPARE_MESSAGE
void messaging_service::register_prepare_message(std::function<future<streaming::messages::prepare_message> (streaming::messages::prepare_message msg, UUID plan_id,
    inet_address from, inet_address connecting, unsigned src_cpu_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::PREPARE_MESSAGE, std::move(func));
}
future<streaming::messages::prepare_message> messaging_service::send_prepare_message(shard_id id, streaming::messages::prepare_message msg, UUID plan_id,
    inet_address from, inet_address connecting, unsigned src_cpu_id, unsigned dst_cpu_id) {
    return send_message_timeout_and_retry<streaming::messages::prepare_message>(this, messaging_verb::PREPARE_MESSAGE, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        std::move(msg), plan_id, from, connecting, src_cpu_id, dst_cpu_id);
}

// PREPARE_DONE_MESSAGE
void messaging_service::register_prepare_done_message(std::function<future<> (UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::PREPARE_DONE_MESSAGE, std::move(func));
}
future<> messaging_service::send_prepare_done_message(shard_id id, UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id) {
    return send_message_timeout_and_retry<void>(this, messaging_verb::PREPARE_DONE_MESSAGE, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        plan_id, from, connecting, dst_cpu_id);
}

// STREAM_MUTATION
void messaging_service::register_stream_mutation(std::function<future<> (UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION, std::move(func));
}
future<> messaging_service::send_stream_mutation(shard_id id, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id) {
    return send_message_timeout_and_retry<void>(this, messaging_verb::STREAM_MUTATION, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        plan_id, std::move(fm), dst_cpu_id);
}

// STREAM_MUTATION_DONE
void messaging_service::register_stream_mutation_done(std::function<future<> (UUID plan_id, UUID cf_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION_DONE, std::move(func));
}
future<> messaging_service::send_stream_mutation_done(shard_id id, UUID plan_id, UUID cf_id, inet_address from, inet_address connecting, unsigned dst_cpu_id) {
    return send_message_timeout_and_retry<void>(this, messaging_verb::STREAM_MUTATION_DONE, id,
        streaming_timeout, streaming_nr_retry, streaming_wait_before_retry,
        plan_id, cf_id, from, connecting, dst_cpu_id);
}

// COMPLETE_MESSAGE
void messaging_service::register_complete_message(std::function<rpc::no_wait_type (UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::COMPLETE_MESSAGE, std::move(func));
}
future<> messaging_service::send_complete_message(shard_id id, UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id) {
    return send_message_oneway(this, messaging_verb::COMPLETE_MESSAGE, std::move(id), std::move(plan_id), std::move(from), std::move(connecting), std::move(dst_cpu_id));
}

void messaging_service::register_echo(std::function<future<> ()>&& func) {
    register_handler(this, messaging_verb::ECHO, std::move(func));
}
void messaging_service::unregister_echo() {
    _rpc->unregister_handler(net::messaging_verb::ECHO);
}
future<> messaging_service::send_echo(shard_id id) {
    return send_message_timeout<void>(this, messaging_verb::ECHO, std::move(id), 3000ms);
}

void messaging_service::register_gossip_shutdown(std::function<rpc::no_wait_type (inet_address from)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_SHUTDOWN, std::move(func));
}
void messaging_service::unregister_gossip_shutdown() {
    _rpc->unregister_handler(net::messaging_verb::GOSSIP_SHUTDOWN);
}
future<> messaging_service::send_gossip_shutdown(shard_id id, inet_address from) {
    return send_message_oneway(this, messaging_verb::GOSSIP_SHUTDOWN, std::move(id), std::move(from));
}

void messaging_service::register_gossip_digest_syn(std::function<future<gossip_digest_ack> (gossip_digest_syn)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_SYN, std::move(func));
}
void messaging_service::unregister_gossip_digest_syn() {
    _rpc->unregister_handler(net::messaging_verb::GOSSIP_DIGEST_SYN);
}
future<gossip_digest_ack> messaging_service::send_gossip_digest_syn(shard_id id, gossip_digest_syn msg) {
    return send_message_timeout<gossip_digest_ack>(this, messaging_verb::GOSSIP_DIGEST_SYN, std::move(id), 3000ms, std::move(msg));
}

void messaging_service::register_gossip_digest_ack2(std::function<rpc::no_wait_type (gossip_digest_ack2)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_ACK2, std::move(func));
}
void messaging_service::unregister_gossip_digest_ack2() {
    _rpc->unregister_handler(net::messaging_verb::GOSSIP_DIGEST_ACK2);
}
future<> messaging_service::send_gossip_digest_ack2(shard_id id, gossip_digest_ack2 msg) {
    return send_message_oneway(this, messaging_verb::GOSSIP_DIGEST_ACK2, std::move(id), std::move(msg));
}

void messaging_service::register_definitions_update(std::function<rpc::no_wait_type (std::vector<frozen_mutation> fm)>&& func) {
    register_handler(this, net::messaging_verb::DEFINITIONS_UPDATE, std::move(func));
}
void messaging_service::unregister_definitions_update() {
    _rpc->unregister_handler(net::messaging_verb::DEFINITIONS_UPDATE);
}
future<> messaging_service::send_definitions_update(shard_id id, std::vector<frozen_mutation> fm) {
    return send_message_oneway(this, messaging_verb::DEFINITIONS_UPDATE, std::move(id), std::move(fm));
}

void messaging_service::register_migration_request(std::function<future<std::vector<frozen_mutation>> (gms::inet_address reply_to, unsigned shard)>&& func) {
    register_handler(this, net::messaging_verb::MIGRATION_REQUEST, std::move(func));
}
void messaging_service::unregister_migration_request() {
    _rpc->unregister_handler(net::messaging_verb::MIGRATION_REQUEST);
}
future<std::vector<frozen_mutation>> messaging_service::send_migration_request(shard_id id, gms::inet_address reply_to, unsigned shard) {
    return send_message<std::vector<frozen_mutation>>(this, messaging_verb::MIGRATION_REQUEST, std::move(id), std::move(reply_to), std::move(shard));
}

void messaging_service::register_mutation(std::function<rpc::no_wait_type (frozen_mutation fm, std::vector<inet_address> forward,
    inet_address reply_to, unsigned shard, response_id_type response_id)>&& func) {
    register_handler(this, net::messaging_verb::MUTATION, std::move(func));
}
void messaging_service::unregister_mutation() {
    _rpc->unregister_handler(net::messaging_verb::MUTATION);
}
future<> messaging_service::send_mutation(shard_id id, const frozen_mutation& fm, std::vector<inet_address> forward,
    inet_address reply_to, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::MUTATION, std::move(id), fm, std::move(forward),
        std::move(reply_to), std::move(shard), std::move(response_id));
}

void messaging_service::register_mutation_done(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id)>&& func) {
    register_handler(this, net::messaging_verb::MUTATION_DONE, std::move(func));
}
void messaging_service::unregister_mutation_done() {
    _rpc->unregister_handler(net::messaging_verb::MUTATION_DONE);
}
future<> messaging_service::send_mutation_done(shard_id id, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::MUTATION_DONE, std::move(id), std::move(shard), std::move(response_id));
}

void messaging_service::register_read_data(std::function<future<foreign_ptr<lw_shared_ptr<query::result>>> (query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_DATA, std::move(func));
}
void messaging_service::unregister_read_data() {
    _rpc->unregister_handler(net::messaging_verb::READ_DATA);
}
future<query::result> messaging_service::send_read_data(shard_id id, query::read_command& cmd, query::partition_range& pr) {
    return send_message<query::result>(this, messaging_verb::READ_DATA, std::move(id), cmd, pr);
}

void messaging_service::register_read_mutation_data(std::function<future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> (query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_MUTATION_DATA, std::move(func));
}
void messaging_service::unregister_read_mutation_data() {
    _rpc->unregister_handler(net::messaging_verb::READ_MUTATION_DATA);
}
future<reconcilable_result> messaging_service::send_read_mutation_data(shard_id id, query::read_command& cmd, query::partition_range& pr) {
    return send_message<reconcilable_result>(this, messaging_verb::READ_MUTATION_DATA, std::move(id), cmd, pr);
}

void messaging_service::register_read_digest(std::function<future<query::result_digest> (query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_DIGEST, std::move(func));
}
void messaging_service::unregister_read_digest() {
    _rpc->unregister_handler(net::messaging_verb::READ_DIGEST);
}
future<query::result_digest> messaging_service::send_read_digest(shard_id id, query::read_command& cmd, query::partition_range& pr) {
    return send_message<query::result_digest>(this, net::messaging_verb::READ_DIGEST, std::move(id), cmd, pr);
}

// Wrapper for TRUNCATE
void messaging_service::register_truncate(std::function<future<> (sstring, sstring)>&& func) {
    register_handler(this, net::messaging_verb::TRUNCATE, std::move(func));
}

void messaging_service::unregister_truncate() {
    _rpc->unregister_handler(net::messaging_verb::TRUNCATE);
}

future<> messaging_service::send_truncate(shard_id id, std::chrono::milliseconds timeout, sstring ks, sstring cf) {
    return send_message_timeout<void>(this, net::messaging_verb::TRUNCATE, std::move(id), std::move(timeout), std::move(ks), std::move(cf));
}

// Wrapper for REPLICATION_FINISHED
void messaging_service::register_replication_finished(std::function<future<> (inet_address)>&& func) {
    register_handler(this, messaging_verb::REPLICATION_FINISHED, std::move(func));
}
void messaging_service::unregister_replication_finished() {
    _rpc->unregister_handler(messaging_verb::REPLICATION_FINISHED);
}
future<> messaging_service::send_replication_finished(shard_id id, inet_address from) {
    // FIXME: getRpcTimeout : conf.request_timeout_in_ms
    return send_message_timeout<void>(this, messaging_verb::REPLICATION_FINISHED, std::move(id), 10000ms, std::move(from));
}

} // namespace net
