#include <boost/intrusive/list.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <iomanip>
#include "core/app-template.hh"
#include "core/async-action.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/vector-data-sink.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include "apps/memcache/ascii.hh"
#include <unistd.h>

#define VERSION_STRING "seastar v1.0"

using namespace net;

namespace bi = boost::intrusive;

namespace memcache {

template<typename T>
using optional = boost::optional<T>;

using item_key = sstring;

struct item_data {
    sstring _data;
    uint32_t _flag;
    clock_type::time_point _expiry;

    optional<uint64_t> as_integral() {
        auto str = _data.c_str();
        if (str[0] == '-') {
            return {};
        }

        auto len = _data.size();

        // Strip trailing space
        while (len && str[len - 1] == ' ') {
            len--;
        }

        try {
            return {boost::lexical_cast<uint64_t>(str, len)};
        } catch (const boost::bad_lexical_cast& e) {
            return {};
        }
    }
};

class item {
private:
    item_data _data;
    uint64_t _version;
    bool _expired;
    bi::list_member_hook<> _timer_link;
    bi::list_member_hook<> _expired_link;
    friend class cache;
public:
    item(item_data data, uint64_t version = 1)
        : _data(std::move(data))
        , _version(version)
        , _expired(false)
    {
    }

    item(const item&) = delete;
    item(item&&) = delete;

    clock_type::time_point get_timeout() {
        return _data._expiry;
    }

    item_data& data() {
        return _data;
    }

    uint64_t version() {
        return _version;
    }
};

struct cache_stats {
    size_t _get_hits {};
    size_t _get_misses {};
    size_t _set_adds {};
    size_t _set_replaces {};
    size_t _cas_hits {};
    size_t _cas_misses {};
    size_t _cas_badval {};
    size_t _delete_misses {};
    size_t _delete_hits {};
    size_t _incr_misses {};
    size_t _incr_hits {};
    size_t _decr_misses {};
    size_t _decr_hits {};
};

enum class cas_result {
    not_found, stored, bad_version
};

class cache {
private:
    using cache_type = std::unordered_map<item_key, shared_ptr<item>>;
    using cache_iterator = typename cache_type::iterator;
    cache_type _cache;
    timer_set<item, &item::_timer_link, clock_type> _alive;

    // Contains items which are present in _cache but have expired
    bi::list<item, bi::member_hook<item, bi::list_member_hook<>, &item::_expired_link>,
        bi::constant_time_size<true>> _expired;

    timer _timer;
    cache_stats _stats;
    timer _flush_timer;
private:
    void expire_now(item& it) {
        it._expired = true;
        _expired.push_back(it);
    }

    void expire() {
        _alive.expire(clock_type::now());
        while (auto item = _alive.pop_expired()) {
            expire_now(*item);
        }
        _timer.arm(_alive.get_next_timeout());
    }

    inline
    cache_iterator find(const item_key& key) {
        auto i = _cache.find(key);
        if (i != _cache.end()) {
            auto& item_ref = *i->second;
            if (item_ref._expired) {
                _expired.erase(_expired.iterator_to(item_ref));
                _cache.erase(i);
                return _cache.end();
            }
        }
        return i;
    }

    inline
    void add_overriding(cache_iterator i, item_data&& data) {
        auto& item_ref = *i->second;
        _alive.remove(item_ref);
        i->second = make_shared<item>(std::move(data), item_ref._version + 1);
        auto& new_ref = *i->second;
        if (_alive.insert(new_ref)) {
            _timer.rearm(new_ref.get_timeout());
        }
    }

    inline
    void add_new(item_key&& key, item_data&& data) {
        auto r = _cache.emplace(std::move(key), make_shared<item>(std::move(data)));
        assert(r.second);
        auto& item_ref = *r.first->second;
        if (_alive.insert(item_ref)) {
            _timer.rearm(item_ref.get_timeout());
        }
    }
public:
    cache() {
        _timer.set_callback([this] { expire(); });
        _flush_timer.set_callback([this] { flush_all(); });
    }

    void flush_all() {
        _flush_timer.cancel();
        for (auto pair : _cache) {
            auto& it = *pair.second;
            if (!it._expired) {
                _alive.remove(it);
                expire_now(it);
            }
        }
    }

    void flush_at(clock_type::time_point time_point) {
        _flush_timer.rearm(time_point);
    }

    bool set(item_key&& key, item_data data) {
        auto i = find(key);
        if (i != _cache.end()) {
            add_overriding(i, std::move(data));
            _stats._set_replaces++;
            return true;
        } else {
            add_new(std::move(key), std::move(data));
            _stats._set_adds++;
            return false;
        }
    }

    bool add(item_key&& key, item_data data) {
        auto i = find(key);
        if (i != _cache.end()) {
            return false;
        }

        _stats._set_adds++;
        add_new(std::move(key), std::move(data));
        return true;
    }

    bool replace(const item_key& key, item_data data) {
        auto i = find(key);
        if (i == _cache.end()) {
            return false;
        }

        _stats._set_replaces++;
        add_overriding(i, std::move(data));
        return true;
    }

    bool remove(const item_key& key) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._delete_misses++;
            return false;
        }
        _stats._delete_hits++;
        auto& item_ref = *i->second;
        _alive.remove(item_ref);
        _cache.erase(i);
        return true;
    }

    shared_ptr<item> get(const item_key& key) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._get_misses++;
            return {};
        }
        _stats._get_hits++;
        return i->second;
    }

    cas_result cas(const item_key& key, uint64_t version, item_data&& data) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._cas_misses++;
            return cas_result::not_found;
        }
        auto& item_ref = *i->second;
        if (item_ref._version != version) {
            _stats._cas_badval++;
            return cas_result::bad_version;
        }
        _stats._cas_hits++;
        add_overriding(i, std::move(data));
        return cas_result::stored;
    }

    size_t size() {
        return _cache.size() - _expired.size();
    }

    cache_stats& stats() {
        return _stats;
    }

    std::pair<shared_ptr<item>, bool> incr(const item_key& key, uint64_t delta) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._incr_misses++;
            return {{}, false};
        }
        auto& item_ref = *i->second;
        _stats._incr_hits++;
        auto value = item_ref._data.as_integral();
        if (!value) {
            return {i->second, false};
        }
        add_overriding(i, item_data{to_sstring(*value + delta), item_ref.data()._flag, item_ref.data()._expiry});
        return {i->second, true};
    }

    std::pair<shared_ptr<item>, bool> decr(const item_key& key, uint64_t delta) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._decr_misses++;
            return {{}, false};
        }
        auto& item_ref = *i->second;
        _stats._decr_hits++;
        auto value = item_ref._data.as_integral();
        if (!value) {
            return {i->second, false};
        }
        add_overriding(i, item_data{to_sstring(*value - std::min(*value, delta)), item_ref.data()._flag, item_ref.data()._expiry});
        return {i->second, true};
    }
};

struct system_stats {
    uint32_t _curr_connections {};
    uint32_t _total_connections {};
    uint64_t _cmd_get {};
    uint64_t _cmd_set {};
    uint64_t _cmd_flush {};
    clock_type::time_point _start_time;
};

class ascii_protocol {
private:
    cache& _cache;
    system_stats& _system_stats;
    memcache_ascii_parser _parser;
private:
    static constexpr uint32_t seconds_in_a_month = 60 * 60 * 24 * 30;
    static constexpr const char *msg_crlf = "\r\n";
    static constexpr const char *msg_error = "ERROR\r\n";
    static constexpr const char *msg_stored = "STORED\r\n";
    static constexpr const char *msg_not_stored = "NOT_STORED\r\n";
    static constexpr const char *msg_end = "END\r\n";
    static constexpr const char *msg_value = "VALUE ";
    static constexpr const char *msg_deleted = "DELETED\r\n";
    static constexpr const char *msg_not_found = "NOT_FOUND\r\n";
    static constexpr const char *msg_ok = "OK\r\n";
    static constexpr const char *msg_version = "VERSION " VERSION_STRING "\r\n";
    static constexpr const char *msg_exists = "EXISTS\r\n";
    static constexpr const char *msg_stat = "STAT ";
    static constexpr const char *msg_error_non_numeric_value = "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n";
private:
    template <bool SendCasVersion>
    future<> handle_get(output_stream<char>& out) {
        _system_stats._cmd_get++;
        auto keys_p = make_shared<std::vector<sstring>>(std::move(_parser._keys));
        return do_for_each(keys_p->begin(), keys_p->end(), [this, &out, keys_p](auto&& key) mutable {
            auto item = _cache.get(key);
            if (!item) {
                return make_ready_future<>();
            }
            return out.write(msg_value)
                    .then([&out, &key] {
                        return out.write(key);
                    }).then([&out] {
                        return out.write(" ");
                    }).then([&out, item] {
                        return out.write(to_sstring(item->data()._flag));
                    }).then([&out] {
                        return out.write(" ");
                    }).then([&out, item] {
                        return out.write(to_sstring(item->data()._data.size()));
                    }).then([&out, item] {
                        if (SendCasVersion) {
                            return out.write(" ").then([&out, item] {
                                return out.write(to_sstring(item->version())).then([&out] {
                                    return out.write(msg_crlf);
                                });
                            });
                        } else {
                            return out.write(msg_crlf);
                        }
                    }).then([&out, item] {
                        return out.write(item->data()._data);
                    }).then([&out] {
                        return out.write(msg_crlf);
                    });
        }).then([&out] {
            return out.write(msg_end);
        });
    }

    template <typename Value>
    static future<> print_stat(output_stream<char>& out, const char* key, Value value) {
        return out.write(msg_stat)
                .then([&out, key] { return out.write(key); })
                .then([&out] { return out.write(" "); })
                .then([&out, value] { return out.write(to_sstring(value)); })
                .then([&out] { return out.write(msg_crlf); });
    }

    future<> print_stats(output_stream<char>& out) {
        auto now = clock_type::now();
        return print_stat(out, "pid", getpid())
            .then([this, now, &out] {
                return print_stat(out, "uptime",
                    std::chrono::duration_cast<std::chrono::seconds>(now - _system_stats._start_time).count());
            }).then([this, now, &out] {
                return print_stat(out, "time",
                    std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
            }).then([this, &out] {
                return print_stat(out, "version", VERSION_STRING);
            }).then([this, &out] {
                return print_stat(out, "pointer_size", sizeof(void*)*8);
            }).then([this, &out, v = _system_stats._curr_connections] {
                return print_stat(out, "curr_connections", v);
            }).then([this, &out, v = _system_stats._total_connections] {
                return print_stat(out, "total_connections", v);
            }).then([this, &out, v = _system_stats._curr_connections] {
                return print_stat(out, "connection_structures", v);
            }).then([this, &out, v = _system_stats._cmd_get] {
                return print_stat(out, "cmd_get", v);
            }).then([this, &out, v = _system_stats._cmd_set] {
                return print_stat(out, "cmd_set", v);
            }).then([this, &out, v = _system_stats._cmd_flush] {
                return print_stat(out, "cmd_flush", v);
            }).then([this, &out] {
                return print_stat(out, "cmd_touch", 0);
            }).then([this, &out, v = _cache.stats()._get_hits] {
                return print_stat(out, "get_hits", v);
            }).then([this, &out, v = _cache.stats()._get_misses] {
                return print_stat(out, "get_misses", v);
            }).then([this, &out, v = _cache.stats()._delete_misses] {
                return print_stat(out, "delete_misses", v);
            }).then([this, &out, v = _cache.stats()._delete_hits] {
                return print_stat(out, "delete_hits", v);
            }).then([this, &out, v = _cache.stats()._incr_misses] {
                return print_stat(out, "incr_misses", v);
            }).then([this, &out, v = _cache.stats()._incr_hits] {
                return print_stat(out, "incr_hits", v);
            }).then([this, &out, v = _cache.stats()._decr_misses] {
                return print_stat(out, "decr_misses", v);
            }).then([this, &out, v = _cache.stats()._decr_hits] {
                return print_stat(out, "decr_hits", v);
            }).then([this, &out, v = _cache.stats()._cas_misses] {
                return print_stat(out, "cas_misses", v);
            }).then([this, &out, v = _cache.stats()._cas_hits] {
                return print_stat(out, "cas_hits", v);
            }).then([this, &out, v = _cache.stats()._cas_badval] {
                return print_stat(out, "cas_badval", v);
            }).then([this, &out] {
                return print_stat(out, "touch_hits", 0);
            }).then([this, &out] {
                return print_stat(out, "touch_misses", 0);
            }).then([this, &out] {
                return print_stat(out, "auth_cmds", 0);
            }).then([this, &out] {
                return print_stat(out, "auth_errors", 0);
            }).then([this, &out] {
                return print_stat(out, "threads", smp::count);
            }).then([this, &out, v = _cache.size()] {
                return print_stat(out, "curr_items", v);
            }).then([this, &out, v = (_cache.stats()._set_replaces + _cache.stats()._set_adds + _cache.stats()._cas_hits)] {
                return print_stat(out, "total_items", v);
            }).then([&out] {
                return out.write("END\r\n");
            });
    }
public:
    ascii_protocol(cache& cache, system_stats& system_stats)
        : _cache(cache)
        , _system_stats(system_stats)
    {}

    clock_type::time_point seconds_to_time_point(uint32_t seconds) {
        if (seconds == 0) {
            return clock_type::time_point::max();
        } else if (seconds <= seconds_in_a_month) {
            return clock_type::now() + std::chrono::seconds(seconds);
        } else {
            return clock_type::time_point(std::chrono::seconds(seconds));
        }
    }

    future<> handle(input_stream<char>& in, output_stream<char>& out) {
        _parser.init();
        return in.consume(_parser).then([this, &out] () -> future<> {
            switch (_parser._state) {
                case memcache_ascii_parser::state::eof:
                    return make_ready_future<>();

                case memcache_ascii_parser::state::error:
                    return out.write(msg_error);

                case memcache_ascii_parser::state::cmd_set:
                    _system_stats._cmd_set++;
                    _cache.set(std::move(_parser._key),
                            item_data{std::move(_parser._blob), _parser._flags, seconds_to_time_point(_parser._expiration)});
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    return out.write(msg_stored);

                case memcache_ascii_parser::state::cmd_cas:
                {
                    _system_stats._cmd_set++;
                    auto result = _cache.cas(_parser._key, _parser._version,
                        item_data{std::move(_parser._blob), _parser._flags, seconds_to_time_point(_parser._expiration)});
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    switch (result) {
                        case cas_result::stored:
                            return out.write(msg_stored);
                        case cas_result::not_found:
                            return out.write(msg_not_found);
                        case cas_result::bad_version:
                            return out.write(msg_exists);
                    }
                }

                case memcache_ascii_parser::state::cmd_add:
                {
                    _system_stats._cmd_set++;
                    auto added = _cache.add(std::move(_parser._key),
                            item_data{std::move(_parser._blob), _parser._flags, seconds_to_time_point(_parser._expiration)});
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    return out.write(added ? msg_stored : msg_not_stored);
                }

                case memcache_ascii_parser::state::cmd_replace:
                {
                    _system_stats._cmd_set++;
                    auto replaced = _cache.replace(std::move(_parser._key),
                            item_data{std::move(_parser._blob), _parser._flags, seconds_to_time_point(_parser._expiration)});
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    return out.write(replaced ? msg_stored : msg_not_stored);
                }

                case memcache_ascii_parser::state::cmd_get:
                    return handle_get<false>(out);

                case memcache_ascii_parser::state::cmd_gets:
                    return handle_get<true>(out);

                case memcache_ascii_parser::state::cmd_delete:
                {
                    auto removed = _cache.remove(_parser._key);
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    return out.write(removed ? msg_deleted : msg_not_found);
                }

                case memcache_ascii_parser::state::cmd_flush_all:
                    _system_stats._cmd_flush++;
                    if (_parser._expiration) {
                        _cache.flush_at(seconds_to_time_point(_parser._expiration));
                    } else {
                        _cache.flush_all();
                    }
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    return out.write(msg_ok);

                case memcache_ascii_parser::state::cmd_version:
                    return out.write(msg_version);

                case memcache_ascii_parser::state::cmd_stats:
                    return print_stats(out);

                case memcache_ascii_parser::state::cmd_incr:
                {
                    auto result = _cache.incr(_parser._key, _parser._u64);
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    auto item = result.first;
                    if (!item) {
                        return out.write(msg_not_found);
                    }
                    auto incremented = result.second;
                    if (!incremented) {
                        return out.write(msg_error_non_numeric_value);
                    }
                    return out.write(item->data()._data).then([&out] {
                        return out.write(msg_crlf);
                    });
                }

                case memcache_ascii_parser::state::cmd_decr:
                {
                    auto result = _cache.decr(_parser._key, _parser._u64);
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    auto item = result.first;
                    if (!item) {
                        return out.write(msg_not_found);
                    }
                    auto decremented = result.second;
                    if (!decremented) {
                        return out.write(msg_error_non_numeric_value);
                    }
                    return out.write(item->data()._data).then([&out] {
                        return out.write(msg_crlf);
                    });
                }
            };
            return make_ready_future<>();
        });
    };
};

void assert_resolved(future<> f) {
    assert(f.available());
}

class udp_server {
public:
    static const size_t default_max_datagram_size = 1400;
private:
    ascii_protocol _proto;
    udp_channel _chan;
    uint16_t _port;
    size_t _max_datagram_size = default_max_datagram_size;

    struct header {
        packed<uint16_t> _request_id;
        packed<uint16_t> _sequence_number;
        packed<uint16_t> _n;
        packed<uint16_t> _reserved;

        template<typename Adjuster>
        auto adjust_endianness(Adjuster a) {
            return a(_request_id, _sequence_number, _n);
        }
    } __attribute__((packed));

public:
    udp_server(cache& c, system_stats& system_stats, uint16_t port = 11211)
         : _proto(c, system_stats)
         , _port(port)
    {}

    void set_max_datagram_size(size_t max_datagram_size) {
        _max_datagram_size = max_datagram_size;
    }

    future<> respond(ipv4_addr dst, uint16_t request_id, std::vector<temporary_buffer<char>>&& datagrams) {
        if (datagrams.size() == 1) {
            auto&& buf = datagrams[0];
            auto p = packet(fragment{buf.get_write(), buf.size()}, buf.release());
            header *out_hdr = p.prepend_header<header>();
            out_hdr->_request_id = request_id;
            out_hdr->_sequence_number = 0;
            out_hdr->_n = 1;
            hton(*out_hdr);
            return _chan.send(dst, std::move(p));
        }

        int i = 0;
        auto sb = make_shared(std::move(datagrams));
        return do_for_each(sb->begin(), sb->end(),
                [this, i, sb, dst, request_id](auto&& buf) mutable {
            auto p = packet(fragment{buf.get_write(), buf.size()}, buf.release());
            header *out_hdr = p.prepend_header<header>();
            out_hdr->_request_id = request_id;
            out_hdr->_sequence_number = i++;
            out_hdr->_n = sb->size();
            hton(*out_hdr);
            return _chan.send(dst, std::move(p));
        });
    }

    void start() {
        _chan = engine.net().make_udp_channel({_port});
        keep_doing([this] {
            return _chan.receive().then([this](udp_datagram dgram) {
                packet& p = dgram.get_data();
                if (p.len() < sizeof(header)) {
                    // dropping invalid packet
                    return make_ready_future<>();
                }

                std::vector<temporary_buffer<char>> out_bufs;
                auto out = output_stream<char>(data_sink(std::make_unique<vector_data_sink>(out_bufs)),
                    _max_datagram_size - sizeof(header));

                header *hdr = p.get_header<header>();
                ntoh(*hdr);
                p.trim_front(sizeof(*hdr));

                auto request_id = hdr->_request_id;

                if (hdr->_n != 1 || hdr->_sequence_number != 0) {
                    out.write("CLIENT_ERROR only single-datagram requests supported\r\n");
                } else {
                    auto in = as_input_stream(std::move(p));
                    assert_resolved(_proto.handle(in, out));
                }

                assert_resolved(out.flush());
                return respond(dgram.get_src(), request_id, std::move(out_bufs));
            });
        }).or_terminate();
    };
};

class tcp_server {
private:
    shared_ptr<server_socket> _listener;
    cache& _cache;
    system_stats& _system_stats;
    uint16_t _port;
    struct connection {
        connected_socket _socket;
        socket_address _addr;
        input_stream<char> _in;
        output_stream<char> _out;
        ascii_protocol _proto;
        system_stats& _system_stats;
        connection(connected_socket&& socket, socket_address addr, cache& c, system_stats& system_stats)
            : _socket(std::move(socket))
            , _addr(addr)
            , _in(_socket.input())
            , _out(_socket.output())
            , _proto(c, system_stats)
            , _system_stats(system_stats)
        {
            _system_stats._curr_connections++;
            _system_stats._total_connections++;
        }
        ~connection() {
            _system_stats._curr_connections--;
        }
    };
public:
    tcp_server(cache& cache, system_stats& system_stats, uint16_t port = 11211)
        : _cache(cache)
        , _system_stats(system_stats)
        , _port(port)
    {}

    void start() {
        listen_options lo;
        lo.reuse_address = true;
        _listener = engine.listen(make_ipv4_address({_port}), lo);
        keep_doing([this] {
            return _listener->accept().then([this] (connected_socket fd, socket_address addr) mutable {
                auto conn = make_shared<connection>(std::move(fd), addr, _cache, _system_stats);
                do_until([conn] { return conn->_in.eof(); }, [this, conn] {
                    return conn->_proto.handle(conn->_in, conn->_out).then([conn] {
                        return conn->_out.flush();
                    });
                });
            });
        }).or_terminate();
    }
};

class stats_printer {
private:
    timer _timer;
    cache& _cache;
public:
    stats_printer(cache& cache)
        : _cache(cache) {}

    void start() {
        _timer.set_callback([this] {
            auto stats = _cache.stats();
            auto gets_total = stats._get_hits + stats._get_misses;
            auto get_hit_rate = gets_total ? ((double)stats._get_hits * 100 / gets_total) : 0;
            auto sets_total = stats._set_adds + stats._set_replaces;
            auto set_replace_rate = sets_total ? ((double)stats._set_replaces * 100/ sets_total) : 0;
            std::cout << "items: " << _cache.size() << " "
                << std::setprecision(2) << std::fixed
                << "get: " << stats._get_hits << "/" << gets_total << " (" << get_hit_rate << "%) "
                << "set: " << stats._set_replaces << "/" << sets_total << " (" <<  set_replace_rate << "%) "
                << std::endl;
        });
        _timer.arm_periodic(std::chrono::seconds(1));
    }
};

} /* namespace memcache */

int main(int ac, char** av)
{
    memcache::cache cache;
    memcache::system_stats system_stats;
    memcache::udp_server udp_server(cache, system_stats);
    memcache::tcp_server tcp_server(cache, system_stats);
    memcache::stats_printer stats(cache);

    system_stats._start_time = clock_type::now();

    app_template app;
    app.add_options()
        ("max-datagram-size", bpo::value<int>()->default_value(memcache::udp_server::default_max_datagram_size),
             "Maximum size of UDP datagram")
        ("stats",
             "Print basic statistics periodically (every second)")
        ;

    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        udp_server.set_max_datagram_size(config["max-datagram-size"].as<int>());
        if (config.count("stats")) {
            stats.start();
        }
        udp_server.start();
        tcp_server.start();
    });
}
