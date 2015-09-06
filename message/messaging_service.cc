/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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

using inet_address = gms::inet_address;
using gossip_digest_syn = gms::gossip_digest_syn;
using gossip_digest_ack = gms::gossip_digest_ack;
using gossip_digest_ack2 = gms::gossip_digest_ack2;
using rpc_protocol = rpc::protocol<serializer, messaging_verb>;
using namespace std::chrono_literals;

template <typename Output>
void net::serializer::write(Output& out, const gms::gossip_digest_syn& v) const {
    return write_gms(out, v);
}
template <typename Input>
gms::gossip_digest_syn net::serializer::read(Input& in, rpc::type<gms::gossip_digest_syn>) const {
    return read_gms<gms::gossip_digest_syn>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const gms::gossip_digest_ack2& v) const {
    return write_gms(out, v);
}
template <typename Input>
gms::gossip_digest_ack2 net::serializer::read(Input& in, rpc::type<gms::gossip_digest_ack2>) const {
    return read_gms<gms::gossip_digest_ack2>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const streaming::messages::stream_init_message& v) const {
    return write_gms(out, v);
}
template <typename Input>
streaming::messages::stream_init_message net::serializer::read(Input& in, rpc::type<streaming::messages::stream_init_message>) const {
    return read_gms<streaming::messages::stream_init_message>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const streaming::messages::prepare_message& v) const {
    return write_gms(out, v);
}
template <typename Input>
streaming::messages::prepare_message net::serializer::read(Input& in, rpc::type<streaming::messages::prepare_message>) const {
    return read_gms<streaming::messages::prepare_message>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const gms::inet_address& v) const {
    return write_gms(out, v);
}
template <typename Input>
gms::inet_address net::serializer::read(Input& in, rpc::type<gms::inet_address>) const {
    return read_gms<gms::inet_address>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const gms::gossip_digest_ack& v) const {
    return write_gms(out, v);
}
template <typename Input>
gms::gossip_digest_ack net::serializer::read(Input& in, rpc::type<gms::gossip_digest_ack>) const {
    return read_gms<gms::gossip_digest_ack>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const query::read_command& v) const {
    return write_gms(out, v);
}
template <typename Input>
query::read_command net::serializer::read(Input& in, rpc::type<query::read_command>) const {
    return read_gms<query::read_command>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const query::result& v) const {
    // FIXME: allow const call to query::result::serialize()
    uint32_t sz = v.serialized_size();
    write(out, sz);
    bytes b(bytes::initialized_later(), sz);
    auto _out = b.begin();
    const_cast<query::result&>(v).serialize(_out);
    out.write(reinterpret_cast<const char*>(b.c_str()), sz);
}
template <typename Input>
query::result net::serializer::read(Input& in, rpc::type<query::result>) const {
    return read_gms<query::result>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const query::result_digest& v) const {
    return write_gms(out, v);
}
template <typename Input>
query::result_digest net::serializer::read(Input& in, rpc::type<query::result_digest>) const {
    return read_gms<query::result_digest>(in);
}

template <typename Output>
void net::serializer::write(Output& out, const utils::UUID& v) const {
    return write_gms(out, v);
}
template <typename Input>
utils::UUID net::serializer::read(Input& in, rpc::type<utils::UUID>) const {
    return read_gms<utils::UUID>(in);
}

// for query::range<T>
template <typename Output, typename T>
void net::serializer::write(Output& out, const query::range<T>& v) const {
    write_gms(out, v);
}
template <typename Input, typename T>
query::range<T> net::serializer::read(Input& in, rpc::type<query::range<T>>) const {
    return read_gms<query::range<T>>(in);
}

struct messaging_service::rpc_protocol_wrapper : public rpc_protocol { using rpc_protocol::rpc_protocol; };

// This wrapper pretends to be rpc_protocol::client, but also handles
// stopping it before destruction, in case it wasn't stopped already.
// This should be integrated into messaging_service proper.
class messaging_service::rpc_protocol_client_wrapper {
    std::unique_ptr<rpc_protocol::client> _p;
    bool _stopped = false;
public:
    rpc_protocol_client_wrapper(rpc_protocol& proto, ipv4_addr addr, ipv4_addr local = ipv4_addr())
            : _p(std::make_unique<rpc_protocol::client>(proto, addr, local)) {
    }
    ~rpc_protocol_client_wrapper() {
        if (_stopped) {
            return;
        }
        auto fut = _p->stop();
        // defer destruction until the "real" client is destroyed
        fut.then_wrapped([p = std::move(_p)] (future<> f) {});
    }
    auto get_stats() const { return _p->get_stats(); }
    future<> stop() {
        if (!_stopped) {
            _stopped = true;
            return _p->stop();
        }
        // FIXME: not really true, a previous stop could be in progress?
        return make_ready_future<>();
    }
    operator rpc_protocol::client&() { return *_p; }
};

struct messaging_service::rpc_protocol_server_wrapper : public rpc_protocol::server { using rpc_protocol::server::server; };

constexpr int32_t messaging_service::current_version;

distributed<messaging_service> _the_messaging_service;

bool operator==(const shard_id& x, const shard_id& y) {
    return x.addr == y.addr && x.cpu_id == y.cpu_id ;
}

bool operator<(const shard_id& x, const shard_id& y) {
    if (x.addr < y.addr) {
        return true;
    } else if (y.addr < x.addr) {
        return false;
    } else {
        return x.cpu_id < y.cpu_id;
    }
}

std::ostream& operator<<(std::ostream& os, const shard_id& x) {
    return os << x.addr << ":" << x.cpu_id;
}

size_t shard_id::hash::operator()(const shard_id& id) const {
    // Ignore the cpu id for now since we do not really support
    // shard to shard connections
    return std::hash<uint32_t>()(id.addr.raw_addr());
}

messaging_service::shard_info::shard_info(shared_ptr<rpc_protocol_client_wrapper>&& client)
    : rpc_client(std::move(client)) {
}

rpc::stats messaging_service::shard_info::get_stats() const {
    return rpc_client->get_stats();
}

void messaging_service::foreach_client(std::function<void(const shard_id& id, const shard_info& info)> f) const {
    for (auto i = _clients.cbegin(); i != _clients.cend(); i++) {
        f(i->first, i->second);
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

messaging_service::messaging_service(gms::inet_address ip)
    : _listen_address(ip)
    , _port(_default_port)
    , _rpc(new rpc_protocol_wrapper(serializer{}))
    , _server(new rpc_protocol_server_wrapper(*_rpc, ipv4_addr{_listen_address.raw_addr(), _port})) {
}

messaging_service::~messaging_service() = default;

uint16_t messaging_service::port() {
    return _port;
}

gms::inet_address messaging_service::listen_address() {
    return _listen_address;
}

future<> messaging_service::stop() {
    return when_all(_server->stop(),
        parallel_for_each(_clients, [](std::pair<const shard_id, shard_info>& c) {
            return c.second.rpc_client->stop();
        })
    ).discard_result();
}

rpc::no_wait_type messaging_service::no_wait() {
    return rpc::no_wait;
}

shared_ptr<messaging_service::rpc_protocol_client_wrapper> messaging_service::get_rpc_client(shard_id id) {
    auto it = _clients.find(id);
    if (it == _clients.end()) {
        auto remote_addr = ipv4_addr(id.addr.raw_addr(), _port);
        auto client = make_shared<rpc_protocol_client_wrapper>(*_rpc, remote_addr, ipv4_addr{_listen_address.raw_addr(), 0});
        it = _clients.emplace(id, shard_info(std::move(client))).first;
        return it->second.rpc_client;
    } else {
        return it->second.rpc_client;
    }
}

void messaging_service::remove_rpc_client(shard_id id) {
    _clients.erase(id);
}

std::unique_ptr<messaging_service::rpc_protocol_wrapper>& messaging_service::rpc() {
    return _rpc;
}

// Register a handler (a callback lambda) for verb
template <typename Func>
void register_handler(messaging_service* ms, messaging_verb verb, Func&& func) {
    ms->rpc()->register_handler(verb, std::move(func));
}

// Send a message for verb
template <typename MsgIn, typename... MsgOut>
auto send_message(messaging_service* ms, messaging_verb verb, shard_id id, MsgOut&&... msg) {
    auto rpc_client_ptr = ms->get_rpc_client(id);
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
            ms->remove_rpc_client(id);
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
    auto rpc_client_ptr = ms->get_rpc_client(id);
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
            ms->remove_rpc_client(id);
            throw;
        } catch (...) {
            // This is expected to be a rpc server error, e.g., the rpc handler throws a std::runtime_error.
            throw;
        }
    });
}

// Send one way message for verb
template <typename... MsgOut>
auto send_message_oneway(messaging_service* ms, messaging_verb verb, shard_id id, MsgOut&&... msg) {
    return send_message<rpc::no_wait_type>(ms, std::move(verb), std::move(id), std::forward<MsgOut>(msg)...);
}

// Wrappers for verbs
void messaging_service::register_stream_init_message(std::function<future<unsigned> (streaming::messages::stream_init_message msg, unsigned src_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_INIT_MESSAGE, std::move(func));
}
future<unsigned> messaging_service::send_stream_init_message(shard_id id, streaming::messages::stream_init_message msg, unsigned src_cpu_id) {
    return send_message<unsigned>(this, messaging_verb::STREAM_INIT_MESSAGE, std::move(id), std::move(msg), std::move(src_cpu_id));
}

void messaging_service::register_prepare_message(std::function<future<streaming::messages::prepare_message> (streaming::messages::prepare_message msg, UUID plan_id,
    inet_address from, inet_address connecting, unsigned src_cpu_id, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::PREPARE_MESSAGE, std::move(func));
}
future<streaming::messages::prepare_message> messaging_service::send_prepare_message(shard_id id, streaming::messages::prepare_message msg, UUID plan_id,
    inet_address from, inet_address connecting, unsigned src_cpu_id, unsigned dst_cpu_id) {
    return send_message<streaming::messages::prepare_message>(this, messaging_verb::PREPARE_MESSAGE, std::move(id), std::move(msg),
            std::move(plan_id), std::move(from), std::move(connecting), std::move(src_cpu_id), std::move(dst_cpu_id));
}

void messaging_service::register_prepare_done_message(std::function<future<> (UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::PREPARE_DONE_MESSAGE, std::move(func));
}

future<> messaging_service::send_prepare_done_message(shard_id id, UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id) {
    return send_message<void>(this, messaging_verb::PREPARE_DONE_MESSAGE, std::move(id), std::move(plan_id), std::move(from), std::move(connecting), std::move(dst_cpu_id));
}

void messaging_service::register_stream_mutation(std::function<future<> (UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION, std::move(func));
}
future<> messaging_service::send_stream_mutation(shard_id id, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id) {
    return send_message<void>(this, messaging_verb::STREAM_MUTATION, std::move(id), std::move(plan_id), std::move(fm), std::move(dst_cpu_id));
}

void messaging_service::register_stream_mutation_done(std::function<future<> (UUID plan_id, UUID cf_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::STREAM_MUTATION_DONE, std::move(func));
}
future<> messaging_service::send_stream_mutation_done(shard_id id, UUID plan_id, UUID cf_id, inet_address from, inet_address connecting, unsigned dst_cpu_id) {
    return send_message<void>(this, messaging_verb::STREAM_MUTATION_DONE, std::move(id), std::move(plan_id), std::move(cf_id), std::move(from), std::move(connecting), std::move(dst_cpu_id));
}

void messaging_service::register_complete_message(std::function<rpc::no_wait_type (UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func) {
    register_handler(this, messaging_verb::COMPLETE_MESSAGE, std::move(func));
}
future<> messaging_service::send_complete_message(shard_id id, UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id) {
    return send_message_oneway(this, messaging_verb::COMPLETE_MESSAGE, std::move(id), std::move(plan_id), std::move(from), std::move(connecting), std::move(dst_cpu_id));
}

void messaging_service::register_echo(std::function<future<> ()>&& func) {
    register_handler(this, messaging_verb::ECHO, std::move(func));
}
future<> messaging_service::send_echo(shard_id id) {
    return send_message<void>(this, messaging_verb::ECHO, std::move(id));
}

void messaging_service::register_gossip_shutdown(std::function<rpc::no_wait_type (inet_address from)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_SHUTDOWN, std::move(func));
}
future<> messaging_service::send_gossip_shutdown(shard_id id, inet_address from) {
    return send_message_oneway(this, messaging_verb::GOSSIP_SHUTDOWN, std::move(id), std::move(from));
}

void messaging_service::register_gossip_digest_syn(std::function<future<gossip_digest_ack> (gossip_digest_syn)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_SYN, std::move(func));
}
future<gossip_digest_ack> messaging_service::send_gossip_digest_syn(shard_id id, gossip_digest_syn msg) {
    return send_message_timeout<gossip_digest_ack>(this, messaging_verb::GOSSIP_DIGEST_SYN, std::move(id), 1000ms, std::move(msg));
}

void messaging_service::register_gossip_digest_ack2(std::function<rpc::no_wait_type (gossip_digest_ack2)>&& func) {
    register_handler(this, messaging_verb::GOSSIP_DIGEST_ACK2, std::move(func));
}
future<> messaging_service::send_gossip_digest_ack2(shard_id id, gossip_digest_ack2 msg) {
    return send_message_oneway(this, messaging_verb::GOSSIP_DIGEST_ACK2, std::move(id), std::move(msg));
}

void messaging_service::register_definitions_update(std::function<rpc::no_wait_type (std::vector<frozen_mutation> fm)>&& func) {
    register_handler(this, net::messaging_verb::DEFINITIONS_UPDATE, std::move(func));
}
future<> messaging_service::send_definitions_update(shard_id id, std::vector<frozen_mutation> fm) {
    return send_message_oneway(this, messaging_verb::DEFINITIONS_UPDATE, std::move(id), std::move(fm));
}

void messaging_service::register_migration_request(std::function<future<std::vector<frozen_mutation>> (gms::inet_address reply_to, unsigned shard)>&& func) {
    register_handler(this, net::messaging_verb::MIGRATION_REQUEST, std::move(func));
}
future<std::vector<frozen_mutation>> messaging_service::send_migration_request(shard_id id, gms::inet_address reply_to, unsigned shard) {
    return send_message<std::vector<frozen_mutation>>(this, messaging_verb::MIGRATION_REQUEST, std::move(id), std::move(reply_to), std::move(shard));
}

void messaging_service::register_mutation(std::function<rpc::no_wait_type (frozen_mutation fm, std::vector<inet_address> forward,
    inet_address reply_to, unsigned shard, response_id_type response_id)>&& func) {
    register_handler(this, net::messaging_verb::MUTATION, std::move(func));
}
future<> messaging_service::send_mutation(shard_id id, const frozen_mutation& fm, std::vector<inet_address> forward,
    inet_address reply_to, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::MUTATION, std::move(id), fm, std::move(forward),
        std::move(reply_to), std::move(shard), std::move(response_id));
}

void messaging_service::register_mutation_done(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id)>&& func) {
    register_handler(this, net::messaging_verb::MUTATION_DONE, std::move(func));
}
future<> messaging_service::send_mutation_done(shard_id id, unsigned shard, response_id_type response_id) {
    return send_message_oneway(this, messaging_verb::MUTATION_DONE, std::move(id), std::move(shard), std::move(response_id));
}

void messaging_service::register_read_data(std::function<future<foreign_ptr<lw_shared_ptr<query::result>>> (query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_DATA, std::move(func));
}
future<query::result> messaging_service::send_read_data(shard_id id, query::read_command& cmd, query::partition_range& pr) {
    return send_message<query::result>(this, messaging_verb::READ_DATA, std::move(id), cmd, pr);
}

void messaging_service::register_read_mutation_data(std::function<future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> (query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_MUTATION_DATA, std::move(func));
}
future<reconcilable_result> messaging_service::send_read_mutation_data(shard_id id, query::read_command& cmd, query::partition_range& pr) {
    return send_message<reconcilable_result>(this, messaging_verb::READ_MUTATION_DATA, std::move(id), cmd, pr);
}

void messaging_service::register_read_digest(std::function<future<query::result_digest> (query::read_command cmd, query::partition_range pr)>&& func) {
    register_handler(this, net::messaging_verb::READ_DIGEST, std::move(func));
}
future<query::result_digest> messaging_service::send_read_digest(shard_id id, query::read_command& cmd, query::partition_range& pr) {
    return send_message<query::result_digest>(this, net::messaging_verb::READ_DIGEST, std::move(id), cmd, pr);
}

} // namespace net
