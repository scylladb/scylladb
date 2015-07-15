/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "message/messaging_service.hh"
#include "core/distributed.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "service/storage_service.hh"

namespace net {

future<> ser_messaging_verb(output_stream<char>& out, messaging_verb& v) {
    bytes b(bytes::initialized_later(), sizeof(v));
    auto _out = b.begin();
    serialize_int32(_out, int32_t(v));
    return out.write(reinterpret_cast<const char*>(b.c_str()), sizeof(v));
}

future<> des_messaging_verb(input_stream<char>& in, messaging_verb& v) {
    return in.read_exactly(sizeof(v)).then([&v] (temporary_buffer<char> buf) mutable {
        if (buf.size() != sizeof(v)) {
            throw rpc::closed_error();
        }
        bytes_view bv(reinterpret_cast<const int8_t*>(buf.get()), sizeof(v));
        v = messaging_verb(read_simple<int32_t>(bv));
    });
}

future<> ser_sstring(output_stream<char>& out, sstring& v) {
    auto serialize_string_size = serialize_int16_size + v.size();
    auto sz = serialize_int16_size + serialize_string_size;
    bytes b(bytes::initialized_later(), sz);
    auto _out = b.begin();
    serialize_int16(_out, serialize_string_size);
    serialize_string(_out, v);
    return out.write(reinterpret_cast<const char*>(b.c_str()), sz);
}

future<> des_sstring(input_stream<char>& in, sstring& v) {
    return in.read_exactly(serialize_int16_size).then([&in, &v] (temporary_buffer<char> buf) mutable {
        if (buf.size() != serialize_int16_size) {
            throw rpc::closed_error();
        }
        size_t serialize_string_size = net::ntoh(*reinterpret_cast<const net::packed<int16_t>*>(buf.get()));
        return in.read_exactly(serialize_string_size).then([serialize_string_size, &v]
            (temporary_buffer<char> buf) mutable {
            if (buf.size() != serialize_string_size) {
                throw rpc::closed_error();
            }
            bytes_view bv(reinterpret_cast<const int8_t*>(buf.get()), serialize_string_size);
            new (&v) sstring(read_simple_short_string(bv));
            return make_ready_future<>();
        });
    });
}

distributed<messaging_service> _the_messaging_service;

future<> deinit_messaging_service() {
    return gms::get_gossiper().stop().then([] {
            return gms::get_failure_detector().stop();
    }).then([] {
            return net::get_messaging_service().stop();
    }).then([]{
            return service::deinit_storage_service();
    });
}

future<> init_messaging_service(sstring listen_address, db::config::seed_provider_type seed_provider) {
    const gms::inet_address listen(listen_address);
    std::set<gms::inet_address> seeds;
    if (seed_provider.parameters.count("seeds") > 0) {
        size_t begin = 0;
        size_t next = 0;
        sstring& seeds_str = seed_provider.parameters.find("seeds")->second;
        while (begin < seeds_str.length() && begin != (next=seeds_str.find(",",begin))) {
            seeds.emplace(gms::inet_address(seeds_str.substr(begin,next-begin)));
            begin = next+1;
        }
    }
    if (seeds.empty()) {
        seeds.emplace(gms::inet_address("127.0.0.1"));
    }

    engine().at_exit([]{
            return deinit_messaging_service();
    });

    return net::get_messaging_service().start(listen).then([seeds] {
        auto& ms = net::get_local_messaging_service();
        print("Messaging server listening on ip %s port %d ...\n", ms.listen_address(), ms.port());
        return gms::get_failure_detector().start().then([seeds] {
            return gms::get_gossiper().start().then([seeds] {
                auto& gossiper = gms::get_local_gossiper();
                gossiper.set_seeds(seeds);
            });
        });
    });
}

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
    return std::hash<uint32_t>()(id.cpu_id) + std::hash<uint32_t>()(id.addr.raw_addr());
}

messaging_service::shard_info::shard_info(std::unique_ptr<rpc::protocol<serializer, messaging_verb>::client>&& client)
    : rpc_client(std::move(client)) {
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
    , _rpc(serializer{})
    , _server(_rpc, ipv4_addr{_listen_address.raw_addr(), _port}) {
}

uint16_t messaging_service::port() {
    return _port;
}

gms::inet_address messaging_service::listen_address() {
    return _listen_address;
}

future<> messaging_service::stop() {
    return when_all(_server.stop(),
        parallel_for_each(_clients, [](std::pair<const shard_id, shard_info>& c) {
            return c.second.rpc_client->stop();
        })
    ).discard_result();
}

rpc::no_wait_type messaging_service::no_wait() {
    return rpc::no_wait;
}

rpc::protocol<serializer, messaging_verb>::client& messaging_service::get_rpc_client(shard_id id) {
    auto it = _clients.find(id);
    if (it == _clients.end()) {
        auto remote_addr = ipv4_addr(id.addr.raw_addr(), _port);
        auto client = std::make_unique<rpc::protocol<serializer, messaging_verb>::client>(_rpc, remote_addr, ipv4_addr{_listen_address.raw_addr(), 0});
        it = _clients.emplace(id, shard_info(std::move(client))).first;
        return *it->second.rpc_client;
    } else {
        return *it->second.rpc_client;
    }
}

void messaging_service::remove_rpc_client(shard_id id) {
    _clients.erase(id);
}

} // namespace net
