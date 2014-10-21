#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <iomanip>
#include "core/app-template.hh"
#include "core/async-action.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/vector-data-sink.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include "apps/memcached/ascii.hh"
#include <unistd.h>

#define VERSION_STRING "seastar v1.0"

using namespace net;

namespace bi = boost::intrusive;

namespace memcache {

template<typename T>
using optional = boost::optional<T>;

using item_key = sstring;

class item {
public:
    using version_type = uint64_t;
private:
    item_key _key;
    const sstring _data;
    const uint32_t _flags;
    version_type _version;
    int _ref_count;
    bi::unordered_set_member_hook<> _cache_link;
    bi::list_member_hook<> _lru_link;
    bi::list_member_hook<> _timer_link;
    clock_type::time_point _expiry;
    friend class cache;
public:
    item(item_key&& key, uint32_t flags, sstring&& data, clock_type::time_point expiry, version_type version = 1)
        : _key(std::move(key))
        , _data(data)
        , _flags(flags)
        , _version(version)
        , _ref_count(0)
        , _expiry(expiry)
    {
    }

    item(const item&) = delete;
    item(item&&) = delete;

    clock_type::time_point get_timeout() {
        return _expiry;
    }

    version_type version() {
        return _version;
    }

    uint32_t flags() {
        return _flags;
    }

    const sstring& data() {
        return _data;
    }

    optional<uint64_t> data_as_integral() {
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

    friend bool operator==(const item &a, const item &b) {
         return a._key == b._key;
    }

    friend std::size_t hash_value(const item &i) {
        return std::hash<item_key>()(i._key);
    }

    friend inline void intrusive_ptr_add_ref(item* it) {
        ++it->_ref_count;
    }

    friend inline void intrusive_ptr_release(item* it) {
        if (--it->_ref_count == 0) {
            delete it;
        }
    }

    friend class item_key_cmp;
};

struct item_key_cmp
{
    bool operator()(const sstring& key, const item &it) const {
        return key == it._key;
    }

    bool operator()(const item& it, const sstring& key) const {
        return it._key == key;
    }
};

using item_ptr = boost::intrusive_ptr<item>;

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
    size_t _expired {};
};

enum class cas_result {
    not_found, stored, bad_version
};

class cache {
private:
    using cache_type = bi::unordered_set<item,
        bi::member_hook<item, bi::unordered_set_member_hook<>, &item::_cache_link>,
        bi::power_2_buckets<true>,
        bi::constant_time_size<true>>;
    using cache_iterator = typename cache_type::iterator;
    static constexpr size_t initial_bucket_count = 1 << 10;
    static constexpr float load_factor = 0.75f;
    size_t _resize_up_threshold = load_factor * initial_bucket_count;
    cache_type::bucket_type* _buckets;
    cache_type _cache;
    timer_set<item, &item::_timer_link, clock_type> _alive;
    bi::list<item, bi::member_hook<item, bi::list_member_hook<>, &item::_lru_link>> _lru;
    timer _timer;
    cache_stats _stats;
    timer _flush_timer;
private:
    void expire() {
        _alive.expire(clock_type::now());
        while (auto item = _alive.pop_expired()) {
            _cache.erase(_cache.iterator_to(*item));
            _lru.erase(_lru.iterator_to(*item));
            intrusive_ptr_release(item);
            _stats._expired++;
        }
        _timer.arm(_alive.get_next_timeout());
    }

    inline
    cache_iterator find(const item_key& key) {
        return _cache.find(key, std::hash<item_key>(), item_key_cmp());
    }

    inline
    cache_iterator add_overriding(cache_iterator i, uint32_t flags, sstring&& data, clock_type::time_point expiry) {
        auto& old_item = *i;
        _alive.remove(old_item);
        _lru.erase(_lru.iterator_to(old_item));
        _cache.erase(_cache.iterator_to(old_item));

        auto new_item = new item(std::move(old_item._key), flags, std::move(data), expiry, old_item._version + 1);
        intrusive_ptr_add_ref(new_item);
        intrusive_ptr_release(&old_item);

        auto insert_result = _cache.insert(*new_item);
        assert(insert_result.second);

        if (_alive.insert(*new_item)) {
            _timer.rearm(new_item->get_timeout());
        }
        _lru.push_front(*new_item);
        return insert_result.first;
    }

    inline
    void add_new(item_key&& key, uint32_t flags, sstring&& data, clock_type::time_point expiry) {
        auto new_item = new item(std::move(key), flags, std::move(data), expiry);
        intrusive_ptr_add_ref(new_item);
        auto& item_ref = *new_item;
        _cache.insert(item_ref);
        if (_alive.insert(item_ref)) {
            _timer.rearm(item_ref.get_timeout());
        }
        _lru.push_front(item_ref);
        maybe_rehash();
    }

    void maybe_rehash() {
        if (_cache.size() >= _resize_up_threshold) {
            auto new_size = _cache.bucket_count() * 2;
            auto old_buckets = _buckets;
            _buckets = new cache_type::bucket_type[new_size];
            _cache.rehash(cache_type::bucket_traits(_buckets, new_size));
            delete[] old_buckets;
            _resize_up_threshold = _cache.bucket_count() * load_factor;
        }
    }
public:
    cache()
        : _buckets(new cache_type::bucket_type[initial_bucket_count])
        , _cache(cache_type::bucket_traits(_buckets, initial_bucket_count))
    {
        _timer.set_callback([this] { expire(); });
        _flush_timer.set_callback([this] { flush_all(); });
    }

    void flush_all() {
        _flush_timer.cancel();
        _cache.erase_and_dispose(_cache.begin(), _cache.end(), [this] (item* it) {
            _alive.remove(*it);
            _lru.erase(_lru.iterator_to(*it));
            intrusive_ptr_release(it);
        });
    }

    void flush_at(clock_type::time_point time_point) {
        _flush_timer.rearm(time_point);
    }

    bool set(item_key&& key, uint32_t flags, sstring&& data, clock_type::time_point expiry) {
        auto i = find(key);
        if (i != _cache.end()) {
            add_overriding(i, flags, std::move(data), std::move(expiry));
            _stats._set_replaces++;
            return true;
        } else {
            add_new(std::move(key), flags, std::move(data), std::move(expiry));
            _stats._set_adds++;
            return false;
        }
    }

    bool add(item_key&& key, uint32_t flags, sstring&& data, clock_type::time_point expiry) {
        auto i = find(key);
        if (i != _cache.end()) {
            return false;
        }

        _stats._set_adds++;
        add_new(std::move(key), flags, std::move(data), expiry);
        return true;
    }

    bool replace(const item_key& key, uint32_t flags, sstring&& data, clock_type::time_point expiry) {
        auto i = find(key);
        if (i == _cache.end()) {
            return false;
        }

        _stats._set_replaces++;
        add_overriding(i, flags, std::move(data), expiry);
        return true;
    }

    bool remove(const item_key& key) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._delete_misses++;
            return false;
        }
        _stats._delete_hits++;
        auto& item_ref = *i;
        _alive.remove(item_ref);
        _lru.erase(_lru.iterator_to(item_ref));
        _cache.erase(i);
        intrusive_ptr_release(&item_ref);
        return true;
    }

    boost::intrusive_ptr<item> get(const item_key& key) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._get_misses++;
            return nullptr;
        }
        _stats._get_hits++;
        auto& item_ref = *i;
        _lru.erase(_lru.iterator_to(item_ref));
        _lru.push_front(item_ref);
        return boost::intrusive_ptr<item>(&item_ref);
    }

    cas_result cas(const item_key& key, item::version_type version,
            uint32_t flags, sstring&& data, clock_type::time_point expiry) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._cas_misses++;
            return cas_result::not_found;
        }
        auto& item_ref = *i;
        if (item_ref._version != version) {
            _stats._cas_badval++;
            return cas_result::bad_version;
        }
        _stats._cas_hits++;
        add_overriding(i, flags, std::move(data), expiry);
        return cas_result::stored;
    }

    size_t size() {
        return _cache.size();
    }

    size_t bucket_count() {
        return _cache.bucket_count();
    }

    cache_stats& stats() {
        return _stats;
    }

    std::pair<item_ptr, bool> incr(const item_key& key, uint64_t delta) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._incr_misses++;
            return {{}, false};
        }
        auto& item_ref = *i;
        _stats._incr_hits++;
        auto value = item_ref.data_as_integral();
        if (!value) {
            return {boost::intrusive_ptr<item>(&item_ref), false};
        }

        i = add_overriding(i, item_ref._flags, to_sstring(*value + delta), item_ref._expiry);
        return {boost::intrusive_ptr<item>(&*i), true};
    }

    std::pair<item_ptr, bool> decr(const item_key& key, uint64_t delta) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._decr_misses++;
            return {{}, false};
        }
        auto& item_ref = *i;
        _stats._decr_hits++;
        auto value = item_ref.data_as_integral();
        if (!value) {
            return {boost::intrusive_ptr<item>(&item_ref), false};
        }
        i = add_overriding(i, item_ref._flags, to_sstring(*value - std::min(*value, delta)), item_ref._expiry);
        return {boost::intrusive_ptr<item>(&*i), true};
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
                    }).then([&out, v = item->flags()] {
                        return out.write(to_sstring(v));
                    }).then([&out] {
                        return out.write(" ");
                    }).then([&out, v = item->data().size()] {
                        return out.write(to_sstring(v));
                    }).then([&out, v = item->version()] {
                        if (SendCasVersion) {
                            return out.write(" ").then([&out, v] {
                                return out.write(to_sstring(v)).then([&out] {
                                    return out.write(msg_crlf);
                                });
                            });
                        } else {
                            return out.write(msg_crlf);
                        }
                    }).then([&out, item] {
                        return out.write(item->data());
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
            }).then([this, &out, v = _cache.bucket_count()] {
                return print_stat(out, "seastar.bucket_count", v);
            }).then([this, &out, v = (double)_cache.size() / _cache.bucket_count()] {
                return print_stat(out, "seastar.load", to_sstring_sprintf(v, "%.2lf"));
            }).then([this, &out, v = _cache.stats()._expired] {
                return print_stat(out, "seastar.expired", v);
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
                    _cache.set(std::move(_parser._key), _parser._flags,
                        std::move(_parser._blob), seconds_to_time_point(_parser._expiration));
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    return out.write(msg_stored);

                case memcache_ascii_parser::state::cmd_cas:
                {
                    _system_stats._cmd_set++;
                    auto result = _cache.cas(_parser._key, _parser._version, _parser._flags,
                        std::move(_parser._blob), seconds_to_time_point(_parser._expiration));
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
                    auto added = _cache.add(std::move(_parser._key), _parser._flags,
                        std::move(_parser._blob), seconds_to_time_point(_parser._expiration));
                    if (_parser._noreply) {
                        return make_ready_future<>();
                    }
                    return out.write(added ? msg_stored : msg_not_stored);
                }

                case memcache_ascii_parser::state::cmd_replace:
                {
                    _system_stats._cmd_set++;
                    auto replaced = _cache.replace(std::move(_parser._key),
                        _parser._flags, std::move(_parser._blob), seconds_to_time_point(_parser._expiration));
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
                    return out.write(item->data()).then([&out] {
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
                    return out.write(item->data()).then([&out] {
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
