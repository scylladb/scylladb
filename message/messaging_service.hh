/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/reactor.hh"
#include "core/iostream.hh"
#include "core/distributed.hh"
#include "core/print.hh"
#include "core/sstring.hh"
#include "net/api.hh"
#include "util/serialization.hh"
#include "gms/inet_address.hh"
#include "rpc/rpc.hh"
#include <unordered_map>
#include "db/config.hh"
#include "frozen_mutation.hh"
#include "db/serializer.hh"

namespace net {

/* All verb handler identifiers */
enum class messaging_verb : int32_t {
    MUTATION,
    MUTATION_DONE,
    BINARY, // Deprecated
    READ_REPAIR,
    READ,
    REQUEST_RESPONSE, // client-initiated reads and writes
    STREAM_INITIATE, // Deprecated
    STREAM_INITIATE_DONE, // Deprecated
    STREAM_REPLY, // Deprecated
    STREAM_REQUEST, // Deprecated
    RANGE_SLICE,
    BOOTSTRAP_TOKEN, // Deprecated
    TREE_REQUEST, // Deprecated
    TREE_RESPONSE, // Deprecated
    JOIN, // Deprecated
    GOSSIP_DIGEST_SYN,
    GOSSIP_DIGEST_ACK,
    GOSSIP_DIGEST_ACK2,
    DEFINITIONS_ANNOUNCE, // Deprecated
    DEFINITIONS_UPDATE,
    TRUNCATE,
    SCHEMA_CHECK,
    INDEX_SCAN, // Deprecated
    REPLICATION_FINISHED,
    INTERNAL_RESPONSE, // responses to internal calls
    COUNTER_MUTATION,
    STREAMING_REPAIR_REQUEST, // Deprecated
    STREAMING_REPAIR_RESPONSE, // Deprecated
    SNAPSHOT, // Similar to nt snapshot
    MIGRATION_REQUEST,
    GOSSIP_SHUTDOWN,
    _TRACE,
    ECHO,
    REPAIR_MESSAGE,
    PAXOS_PREPARE,
    PAXOS_PROPOSE,
    PAXOS_COMMIT,
    PAGED_RANGE,
    UNUSED_1,
    UNUSED_2,
    UNUSED_3,
    // Used by streaming
    STREAM_INIT_MESSAGE,
    PREPARE_MESSAGE,
    INCOMING_FILE_MESSAGE,
    OUTGOING_FILE_MESSAGE,
    RECEIVED_MESSAGE,
    RETRY_MESSAGE,
    COMPLETE_MESSAGE,
    SESSION_FAILED_MESSAGE,
    LAST,
};

} // namespace net

namespace std {
template <>
class hash<net::messaging_verb> {
public:
    size_t operator()(const net::messaging_verb& x) const {
        return hash<int32_t>()(int32_t(x));
    }
};
} // namespace std

namespace net {

// NOTE: operator(input_stream<char>&, T&) takes a reference to uninitialized
//       T object and should use placement new in case T is non POD
struct serializer {
    // For integer type
    template<typename T>
    inline auto operator()(output_stream<char>& out, T&& v, std::enable_if_t<std::is_integral<std::remove_reference_t<T>>::value, void*> = nullptr) {
        auto v_ = net::hton(v);
        return out.write(reinterpret_cast<const char*>(&v_), sizeof(T));
    }
    template<typename T>
    inline auto operator()(input_stream<char>& in, T& v, std::enable_if_t<std::is_integral<T>::value, void*> = nullptr) {
        return in.read_exactly(sizeof(v)).then([&v] (temporary_buffer<char> buf) mutable {
            if (buf.size() != sizeof(v)) {
                throw rpc::closed_error();
            }
            v = net::ntoh(*reinterpret_cast<const net::packed<T>*>(buf.get()));
        });
    }

    // For vectors
    template<typename T>
    inline auto operator()(output_stream<char>& out, std::vector<T>& v) {
        return operator()(out, v.size()).then([&out, &v, this] {
            return do_for_each(v.begin(), v.end(), [&out, this] (T& e) {
                return operator()(out, e);
            });
        });
    }
    template<typename T>
    inline auto operator()(input_stream<char>& in, std::vector<T>& v) {
        using size_type = typename  std::vector<T>::size_type;
        return in.read_exactly(sizeof(size_type)).then([&v, &in, this] (temporary_buffer<char> buf) {
            if (buf.size() != sizeof(size_type)) {
                throw rpc::closed_error();
            }
            size_type c = net::ntoh(*reinterpret_cast<const net::packed<size_type>*>(buf.get()));
            new (&v) std::vector<T>;
            v.reserve(c);
            union U {
                U(){}
                ~U(){}
                T v;
            };
            return do_until([c] () mutable {return !c--;}, [&v, &in, u = U(), this] () mutable {
                return operator()(in, u.v).then([&u, &v] {
                    v.emplace_back(std::move(u.v));
                });
            });
        });
    }

    // For messaging_verb
    inline auto operator()(output_stream<char>& out, messaging_verb& v) {
        bytes b(bytes::initialized_later(), sizeof(v));
        auto _out = b.begin();
        serialize_int32(_out, int32_t(v));
        return out.write(reinterpret_cast<const char*>(b.c_str()), sizeof(v));
    }
    inline auto operator()(input_stream<char>& in, messaging_verb& v) {
        return in.read_exactly(sizeof(v)).then([&v] (temporary_buffer<char> buf) mutable {
            if (buf.size() != sizeof(v)) {
                throw rpc::closed_error();
            }
            bytes_view bv(reinterpret_cast<const int8_t*>(buf.get()), sizeof(v));
            v = messaging_verb(read_simple<int32_t>(bv));
        });
    }

    // For sstring
    inline auto operator()(output_stream<char>& out, sstring& v) {
        auto serialize_string_size = serialize_int16_size + v.size();
        auto sz = serialize_int16_size + serialize_string_size;
        bytes b(bytes::initialized_later(), sz);
        auto _out = b.begin();
        serialize_int16(_out, serialize_string_size);
        serialize_string(_out, v);
        return out.write(reinterpret_cast<const char*>(b.c_str()), sz);
    }
    inline auto operator()(input_stream<char>& in, sstring& v) {
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

    // For frozen_mutation
    inline auto operator()(output_stream<char>& out, const frozen_mutation& v) {
        db::frozen_mutation_serializer s(v);
        uint32_t sz = s.size() + data_output::serialized_size(sz);
        bytes b(bytes::initialized_later(), sz);
        data_output o(b);
        o.write<uint32_t>(sz - data_output::serialized_size(sz));
        db::frozen_mutation_serializer::write(o, v);
        return out.write(reinterpret_cast<const char*>(b.c_str()), sz);
    }
    inline auto operator()(input_stream<char>& in, frozen_mutation& v) {
        static auto sz = data_output::serialized_size<uint32_t>();
        return in.read_exactly(sz).then([&v, &in] (temporary_buffer<char> buf) mutable {
            if (buf.size() != sz) {
                throw rpc::closed_error();
            }
            data_input i(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), sz));
            size_t msz = i.read<int32_t>();
            return in.read_exactly(msz).then([&v, msz] (temporary_buffer<char> buf) {
                if (buf.size() != msz) {
                    throw rpc::closed_error();
                }
                data_input i(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), msz));
                new (&v) frozen_mutation(db::frozen_mutation_serializer::read(i));
            });
        });
    }

    // For complex types which have serialize()/deserialize(),  e.g. gms::gossip_digest_syn, gms::gossip_digest_ack2
    template<typename T>
    inline auto operator()(output_stream<char>& out, T&& v, std::enable_if_t<!std::is_integral<std::remove_reference_t<T>>::value &&
                                                                             !std::is_enum<std::remove_reference_t<T>>::value, void*> = nullptr) {
        auto sz = serialize_int32_size + v.serialized_size();
        bytes b(bytes::initialized_later(), sz);
        auto _out = b.begin();
        serialize_int32(_out, int32_t(sz - serialize_int32_size));
        v.serialize(_out);
        return out.write(reinterpret_cast<const char*>(b.c_str()), sz);
    }
    template<typename T>
    inline auto operator()(input_stream<char>& in, T& v, std::enable_if_t<!std::is_integral<T>::value &&
                                                                          !std::is_enum<T>::value, void*> = nullptr) {
        return in.read_exactly(serialize_int32_size).then([&in, &v] (temporary_buffer<char> buf) mutable {
            if (buf.size() != serialize_int32_size) {
                throw rpc::closed_error();
            }
            size_t sz = net::ntoh(*reinterpret_cast<const net::packed<int32_t>*>(buf.get()));
            return in.read_exactly(sz).then([sz, &v] (temporary_buffer<char> buf) mutable {
                if (buf.size() != sz) {
                    throw rpc::closed_error();
                }
                bytes_view bv(reinterpret_cast<const int8_t*>(buf.get()), sz);
                new (&v) T(T::deserialize(bv));
                return make_ready_future<>();
            });
        });
    }
};

class messaging_service {
public:
    struct shard_id {
        gms::inet_address addr;
        uint32_t cpu_id;
        friend inline bool operator==(const shard_id& x, const shard_id& y) {
            return x.addr == y.addr && x.cpu_id == y.cpu_id ;
        }
        friend inline bool operator<(const shard_id& x, const shard_id& y) {
            if (x.addr < y.addr) {
                return true;
            } else if (y.addr < x.addr) {
                return false;
            } else {
                return x.cpu_id < y.cpu_id;
            }
        }
        friend inline std::ostream& operator<<(std::ostream& os, const shard_id& x) {
            return os << x.addr << ":" << x.cpu_id;
        }
        struct hash {
            size_t operator()(const shard_id& id) const {
                return std::hash<uint32_t>()(id.cpu_id) + std::hash<uint32_t>()(id.addr.raw_addr());
            }
        };
    };
    struct shard_info {
        shard_info(std::unique_ptr<rpc::protocol<serializer, messaging_verb>::client>&& client)
            : rpc_client(std::move(client)) {
        }
        std::unique_ptr<rpc::protocol<serializer, messaging_verb>::client> rpc_client;
    };

    void foreach_client(std::function<void(const messaging_service::shard_id& id,
                    const messaging_service::shard_info& info)> f) const {
        for (auto i = _clients.cbegin(); i != _clients.cend(); i++) {
            f(i->first, i->second);
        }
    }

    void increment_dropped_messages(messaging_verb verb) {
        _dropped_messages[static_cast<int32_t>(verb)]++;
    }

    uint64_t get_dropped_messages(messaging_verb verb) const {
        return _dropped_messages[static_cast<int32_t>(verb)];
    }

    const uint64_t* get_dropped_messages() const {
        return _dropped_messages;
    }


private:
    static constexpr uint16_t _default_port = 7000;
    gms::inet_address _listen_address;
    uint16_t _port;
    rpc::protocol<serializer, messaging_verb> _rpc;
    rpc::protocol<serializer, messaging_verb>::server _server;
    std::unordered_map<shard_id, shard_info, shard_id::hash> _clients;
    uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::LAST)] = {};
public:
    messaging_service(gms::inet_address ip = gms::inet_address("0.0.0.0"))
        : _listen_address(ip)
        , _port(_default_port)
        , _rpc(serializer{})
        , _server(_rpc, ipv4_addr{_listen_address.raw_addr(), _port}) {
    }
public:
    uint16_t port() {
        return _port;
    }
    auto listen_address() {
        return _listen_address;
    }
    future<> stop() {
        return make_ready_future<>();
    }

    static auto no_wait() {
        return rpc::no_wait;
    }
public:
    // Register a handler (a callback lambda) for verb
    template <typename Func>
    void register_handler(messaging_verb verb, Func&& func) {
        _rpc.register_handler(verb, std::move(func));
    }

    // Send a message for verb
    template <typename MsgIn, typename... MsgOut>
    auto send_message(messaging_verb verb, shard_id id, MsgOut&&... msg) {
        auto& rpc_client = get_rpc_client(id);
        auto rpc_handler = _rpc.make_client<MsgIn(MsgOut...)>(verb);
        return rpc_handler(rpc_client, std::forward<MsgOut>(msg)...).then_wrapped([this, id, verb] (auto&& f) {
            try {
                if (f.failed()) {
                    this->increment_dropped_messages(verb);
                    f.get();
                    assert(false); // never reached
                }
                return std::move(f);
            } catch(...) {
                // FIXME: we need to distinguish between a transport error and
                // a server error.
                // remove_rpc_client(id);
                throw;
            }
        });
    }

    template <typename... MsgOut>
    auto send_message_oneway(messaging_verb verb, shard_id id, MsgOut&&... msg) {
        return send_message<rpc::no_wait_type>(std::move(verb), std::move(id), std::forward<MsgOut>(msg)...);
    }
private:
    // Return rpc::protocol::client for a shard which is a ip + cpuid pair.
    rpc::protocol<serializer, messaging_verb>::client& get_rpc_client(shard_id id) {
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

    void remove_rpc_client(shard_id id) {
        _clients.erase(id);
    }
};

extern distributed<messaging_service> _the_messaging_service;

inline distributed<messaging_service>& get_messaging_service() {
    return _the_messaging_service;
}

inline messaging_service& get_local_messaging_service() {
    return _the_messaging_service.local();
}

future<> init_messaging_service(sstring listen_address, db::config::seed_provider_type seed_provider);
} // namespace net
