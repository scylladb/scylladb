#include <boost/intrusive/list.hpp>
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
    item(item_data data)
        : _data(std::move(data))
        , _version(1)
        , _expired(false)
    {
    }

    clock_type::time_point get_timeout() {
        return _data._expiry;
    }

    void update(item_data&& data) {
        _data = std::move(data);
        _version++;
    }

    item_data& data() {
        return _data;
    }
};

struct cache_stats {
    size_t _get_hits {};
    size_t _get_misses {};
    size_t _set_adds {};
    size_t _set_replaces {};
};

class cache {
private:
    using cache_type = std::unordered_map<item_key, shared_ptr<item>>;
    using cache_iterator = typename cache_type::iterator;
    cache_type _cache;
    timer_set<item, &item::_timer_link, clock_type> _alive;
    bi::list<item, bi::member_hook<item, bi::list_member_hook<>, &item::_expired_link>> _expired;
    timer _timer;
    cache_stats _stats;
private:
    void expire() {
        _alive.expire(clock_type::now());
        while (auto item = _alive.pop_expired()) {
            item->_expired = true;
            _expired.push_back(*item);
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
        item_ref.update(std::move(data));
        if (_alive.insert(item_ref)) {
            _timer.rearm(item_ref.get_timeout());
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

        add_new(std::move(key), std::move(data));
        return true;
    }

    bool replace(const item_key& key, item_data data) {
        auto i = find(key);
        if (i == _cache.end()) {
            return false;
        }

        add_overriding(i, std::move(data));
        return true;
    }

    bool remove(const item_key& key) {
        auto i = find(key);
        if (i == _cache.end()) {
            return false;
        }
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

    size_t size() {
        return _cache.size();
    }

    cache_stats& stats() {
        return _stats;
    }
};

class ascii_protocol {
private:
    cache& _cache;
    memcache_ascii_parser _parser;
private:
    static constexpr uint32_t seconds_in_a_month = 60 * 60 * 24 * 30;
    static constexpr const char *msg_crlf = "\r\n";
    static constexpr const char *msg_error = "ERROR\r\n";
    static constexpr const char *msg_stored = "STORED\r\n";
    static constexpr const char *msg_end = "END\r\n";
    static constexpr const char *msg_value = "VALUE ";
    static constexpr const char *msg_deleted = "DELETED\r\n";
    static constexpr const char *msg_not_found = "NOT_FOUND\r\n";
public:
    ascii_protocol(cache& cache) : _cache(cache) {}

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
                case memcache_ascii_parser::state::error:
                case memcache_ascii_parser::state::eof:
                    return out.write(msg_error);

                case memcache_ascii_parser::state::cmd_set:
                    _cache.set(std::move(_parser._key),
                            item_data{std::move(_parser._blob), _parser._flags, seconds_to_time_point(_parser._expiration)});
                    return out.write(msg_stored);

                case memcache_ascii_parser::state::cmd_get:
                {
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
                                }).then([&out] {
                                    return out.write(msg_crlf);
                                }).then([&out, item] {
                                    return out.write(item->data()._data);
                                }).then([&out] {
                                    return out.write(msg_crlf);
                                });
                    }).then([&out] {
                        return out.write(msg_end);
                    });
                }

                case memcache_ascii_parser::state::cmd_delete:
                    if (_cache.remove(_parser._key)) {
                        return out.write(msg_deleted);
                    }
                    return out.write(msg_not_found);
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
    ascii_protocol& _proto;
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
    udp_server(ascii_protocol& proto, uint16_t port = 11211)
         : _proto(proto)
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
    memcache::ascii_protocol ascii_protocol(cache);
    memcache::udp_server server(ascii_protocol);
    memcache::stats_printer stats(cache);

    app_template app;
    app.add_options()
        ("max-datagram-size", bpo::value<int>()->default_value(memcache::udp_server::default_max_datagram_size),
             "Maximum size of UDP datagram")
        ("stats",
             "Print basic statistics periodically (every second)")
        ;

    return app.run(ac, av, [&server, &stats, &app] {
        auto&& config = app.configuration();
        server.set_max_datagram_size(config["max-datagram-size"].as<int>());
        if (config.count("stats")) {
            stats.start();
        }
        server.start();
    });
}
