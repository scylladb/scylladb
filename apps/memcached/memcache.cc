/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014-2015 Cloudius Systems
 */

#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/optional.hpp>
#include <iomanip>
#include <sstream>
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/timer-set.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "core/memory.hh"
#include "core/units.hh"
#include "core/distributed.hh"
#include "core/vector-data-sink.hh"
#include "core/bitops.hh"
#include "core/slab.hh"
#include "core/align.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include "apps/memcached/ascii.hh"
#include "memcached.hh"
#include <unistd.h>

#define PLATFORM "seastar"
#define VERSION "v1.0"
#define VERSION_STRING PLATFORM " " VERSION

using namespace net;

namespace bi = boost::intrusive;

namespace memcache {

static constexpr double default_slab_growth_factor = 1.25;
static constexpr uint64_t default_slab_page_size = 1UL*MB;
static constexpr uint64_t default_per_cpu_slab_size = 0UL; // zero means reclaimer is enabled.
static __thread slab_allocator<item>* slab;

template<typename T>
using optional = boost::optional<T>;

struct expiration {
    static constexpr uint32_t seconds_in_a_month = 60U * 60 * 24 * 30;
    uint32_t _time;

    expiration() : _time(0U) {}

    expiration(uint32_t seconds) {
        if (seconds == 0U) {
            _time = 0U; // means never expire.
        } else if (seconds <= seconds_in_a_month) {
            _time = seconds + time(0); // from delta
        } else {
            _time = seconds; // from real time
        }
    }

    bool ever_expires() {
        return _time;
    }

    clock_type::time_point to_time_point() {
        return clock_type::time_point(std::chrono::seconds(_time));
    }
};

class item : public slab_item_base {
public:
    using version_type = uint64_t;
    using time_point = clock_type::time_point;
    using duration = clock_type::duration;
    static constexpr uint8_t field_alignment = alignof(void*);
private:
    using hook_type = bi::unordered_set_member_hook<>;
    // TODO: align shared data to cache line boundary
    version_type _version;
    hook_type _cache_link;
    bi::list_member_hook<> _timer_link;
    size_t _key_hash;
    expiration _expiry;
    uint32_t _value_size;
    uint32_t _slab_page_index;
    uint16_t _ref_count;
    uint8_t _key_size;
    uint8_t _ascii_prefix_size;
    char _data[]; // layout: data=key, (data+key_size)=ascii_prefix, (data+key_size+ascii_prefix_size)=value.
    friend class cache;
public:
    item(uint32_t slab_page_index, item_key&& key, sstring&& ascii_prefix,
         sstring&& value, expiration expiry, version_type version = 1)
        : _version(version)
        , _key_hash(key.hash())
        , _expiry(expiry)
        , _value_size(value.size())
        , _slab_page_index(slab_page_index)
        , _ref_count(0U)
        , _key_size(key.key().size())
        , _ascii_prefix_size(ascii_prefix.size())
    {
        assert(_key_size <= std::numeric_limits<uint8_t>::max());
        assert(_ascii_prefix_size <= std::numeric_limits<uint8_t>::max());
        // storing key
        memcpy(_data, key.key().c_str(), _key_size);
        // storing ascii_prefix
        memcpy(_data + align_up(_key_size, field_alignment), ascii_prefix.c_str(), _ascii_prefix_size);
        // storing value
        memcpy(_data + align_up(_key_size, field_alignment) + align_up(_ascii_prefix_size, field_alignment),
               value.c_str(), _value_size);
    }

    item(const item&) = delete;
    item(item&&) = delete;

    clock_type::time_point get_timeout() {
        return _expiry.to_time_point();
    }

    version_type version() {
        return _version;
    }

    const std::experimental::string_view key() const {
        return std::experimental::string_view(_data, _key_size);
    }

    const std::experimental::string_view ascii_prefix() const {
        const char *p = _data + align_up(_key_size, field_alignment);
        return std::experimental::string_view(p, _ascii_prefix_size);
    }

    const std::experimental::string_view value() const {
        const char *p = _data + align_up(_key_size, field_alignment) +
            align_up(_ascii_prefix_size, field_alignment);
        return std::experimental::string_view(p, _value_size);
    }

    size_t key_size() const {
        return _key_size;
    }

    size_t ascii_prefix_size() const {
        return _ascii_prefix_size;
    }

    size_t value_size() const {
        return _value_size;
    }

    optional<uint64_t> data_as_integral() {
        auto str = value().data();
        if (str[0] == '-') {
            return {};
        }

        auto len = _value_size;

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

    // needed by timer_set
    bool cancel() {
        return false;
    }

    // Methods required by slab allocator.
    uint32_t get_slab_page_index() const {
        return _slab_page_index;
    }
    bool is_unlocked() const {
        return _ref_count == 1;
    }

    friend bool operator==(const item &a, const item &b) {
         return (a._key_hash == b._key_hash) &&
            (a._key_size == b._key_size) &&
            (memcmp(a._data, b._data, a._key_size) == 0);
    }

    friend std::size_t hash_value(const item &i) {
        return i._key_hash;
    }

    friend inline void intrusive_ptr_add_ref(item* it) {
        assert(it->_ref_count >= 0);
        ++it->_ref_count;
        if (it->_ref_count == 2) {
            slab->lock_item(it);
        }
    }

    friend inline void intrusive_ptr_release(item* it) {
        --it->_ref_count;
        if (it->_ref_count == 1) {
            slab->unlock_item(it);
        } else if (it->_ref_count == 0) {
            slab->free(it);
        }
        assert(it->_ref_count >= 0);
    }

    friend class item_key_cmp;
};

struct item_key_cmp
{
private:
    bool compare(const item_key& key, const item& it) const {
        return (it._key_hash == key.hash()) &&
            (it._key_size == key.key().size()) &&
            (memcmp(it._data, key.key().c_str(), it._key_size) == 0);
    }
public:
    bool operator()(const item_key& key, const item& it) const {
        return compare(key, it);
    }

    bool operator()(const item& it, const item_key& key) const {
        return compare(key, it);
    }
};

using item_ptr = foreign_ptr<boost::intrusive_ptr<item>>;

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
    size_t _evicted {};
    size_t _bytes {};
    size_t _resize_failure {};
    size_t _size {};
    size_t _reclaims{};

    void operator+=(const cache_stats& o) {
        _get_hits += o._get_hits;
        _get_misses += o._get_misses;
        _set_adds += o._set_adds;
        _set_replaces += o._set_replaces;
        _cas_hits += o._cas_hits;
        _cas_misses += o._cas_misses;
        _cas_badval += o._cas_badval;
        _delete_misses += o._delete_misses;
        _delete_hits += o._delete_hits;
        _incr_misses += o._incr_misses;
        _incr_hits += o._incr_hits;
        _decr_misses += o._decr_misses;
        _decr_hits += o._decr_hits;
        _expired += o._expired;
        _evicted += o._evicted;
        _bytes += o._bytes;
        _resize_failure += o._resize_failure;
        _size += o._size;
        _reclaims += o._reclaims;
    }
};

enum class cas_result {
    not_found, stored, bad_version
};

struct remote_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return ref;
    }
};

struct local_origin_tag {
    template <typename T>
    static inline
    T move_if_local(T& ref) {
        return std::move(ref);
    }
};

struct item_insertion_data {
    item_key key;
    sstring ascii_prefix;
    sstring data;
    expiration expiry;
};

class cache {
private:
    using cache_type = bi::unordered_set<item,
        bi::member_hook<item, item::hook_type, &item::_cache_link>,
        bi::power_2_buckets<true>,
        bi::constant_time_size<true>>;
    using cache_iterator = typename cache_type::iterator;
    static constexpr size_t initial_bucket_count = 1 << 10;
    static constexpr float load_factor = 0.75f;
    size_t _resize_up_threshold = load_factor * initial_bucket_count;
    cache_type::bucket_type* _buckets;
    cache_type _cache;
    seastar::timer_set<item, &item::_timer_link> _alive;
    timer<> _timer;
    cache_stats _stats;
    timer<> _flush_timer;
private:
    size_t item_size(item& item_ref) {
        constexpr size_t field_alignment = alignof(void*);
        return sizeof(item) +
            align_up(item_ref.key_size(), field_alignment) +
            align_up(item_ref.ascii_prefix_size(), field_alignment) +
            item_ref.value_size();
    }

    size_t item_size(item_insertion_data& insertion) {
        constexpr size_t field_alignment = alignof(void*);
        auto size = sizeof(item) +
            align_up(insertion.key.key().size(), field_alignment) +
            align_up(insertion.ascii_prefix.size(), field_alignment) +
            insertion.data.size();
#ifdef __DEBUG__
        static bool print_item_footprint = true;
        if (print_item_footprint) {
            print_item_footprint = false;
            std::cout << __FUNCTION__ << ": " << size << "\n";
            std::cout << "sizeof(item)      " << sizeof(item) << "\n";
            std::cout << "key.size          " << insertion.key.key().size() << "\n";
            std::cout << "value.size        " << insertion.data.size() << "\n";
            std::cout << "ascii_prefix.size " << insertion.ascii_prefix.size() << "\n";
        }
#endif
        return size;
    }

    template <bool IsInCache = true, bool IsInTimerList = true, bool Release = true>
    void erase(item& item_ref) {
        if (IsInCache) {
            _cache.erase(_cache.iterator_to(item_ref));
        }
        if (IsInTimerList) {
            if (item_ref._expiry.ever_expires()) {
                _alive.remove(item_ref);
            }
        }
        _stats._bytes -= item_size(item_ref);
        if (Release) {
            // memory used by item shouldn't be freed when slab is replacing it with another item.
            intrusive_ptr_release(&item_ref);
        }
    }

    void expire() {
        auto exp = _alive.expire(clock_type::now());
        while (!exp.empty()) {
            auto item = &*exp.begin();
            exp.pop_front();
            erase<true, false>(*item);
            _stats._expired++;
        }
        _timer.arm(_alive.get_next_timeout());
    }

    inline
    cache_iterator find(const item_key& key) {
        return _cache.find(key, std::hash<item_key>(), item_key_cmp());
    }

    template <typename Origin>
    inline
    cache_iterator add_overriding(cache_iterator i, item_insertion_data& insertion) {
        auto& old_item = *i;
        uint64_t old_item_version = old_item._version;

        erase(old_item);

        size_t size = item_size(insertion);
        auto new_item = slab->create(size, Origin::move_if_local(insertion.key), Origin::move_if_local(insertion.ascii_prefix),
            Origin::move_if_local(insertion.data), insertion.expiry, old_item_version + 1);
        intrusive_ptr_add_ref(new_item);

        auto insert_result = _cache.insert(*new_item);
        assert(insert_result.second);
        if (insertion.expiry.ever_expires() && _alive.insert(*new_item)) {
            _timer.rearm(new_item->get_timeout());
        }
        _stats._bytes += size;
        return insert_result.first;
    }

    template <typename Origin>
    inline
    void add_new(item_insertion_data& insertion) {
        size_t size = item_size(insertion);
        auto new_item = slab->create(size, Origin::move_if_local(insertion.key), Origin::move_if_local(insertion.ascii_prefix),
            Origin::move_if_local(insertion.data), insertion.expiry);
        intrusive_ptr_add_ref(new_item);
        auto& item_ref = *new_item;
        _cache.insert(item_ref);
        if (insertion.expiry.ever_expires() && _alive.insert(item_ref)) {
            _timer.rearm(item_ref.get_timeout());
        }
        _stats._bytes += size;
        maybe_rehash();
    }

    void maybe_rehash() {
        if (_cache.size() >= _resize_up_threshold) {
            auto new_size = _cache.bucket_count() * 2;
            auto old_buckets = _buckets;
            try {
                _buckets = new cache_type::bucket_type[new_size];
            } catch (const std::bad_alloc& e) {
                _stats._resize_failure++;
                return;
            }
            _cache.rehash(typename cache_type::bucket_traits(_buckets, new_size));
            delete[] old_buckets;
            _resize_up_threshold = _cache.bucket_count() * load_factor;
        }
    }
public:
    cache(uint64_t per_cpu_slab_size, uint64_t slab_page_size)
        : _buckets(new cache_type::bucket_type[initial_bucket_count])
        , _cache(cache_type::bucket_traits(_buckets, initial_bucket_count))
    {
        _timer.set_callback([this] { expire(); });
        _flush_timer.set_callback([this] { flush_all(); });

        // initialize per-thread slab allocator.
        slab = new slab_allocator<item>(default_slab_growth_factor, per_cpu_slab_size, slab_page_size,
                [this](item& item_ref) { erase<true, true, false>(item_ref); _stats._evicted++; });
#ifdef __DEBUG__
        static bool print_slab_classes = true;
        if (print_slab_classes) {
            print_slab_classes = false;
            slab->print_slab_classes();
        }
#endif
    }

    ~cache() {
       flush_all();
    }

    void flush_all() {
        _flush_timer.cancel();
        _cache.erase_and_dispose(_cache.begin(), _cache.end(), [this] (item* it) {
            erase<false, true>(*it);
        });
    }

    void flush_at(clock_type::time_point time_point) {
        _flush_timer.rearm(time_point);
    }

    template <typename Origin = local_origin_tag>
    bool set(item_insertion_data& insertion) {
        auto i = find(insertion.key);
        if (i != _cache.end()) {
            add_overriding<Origin>(i, insertion);
            _stats._set_replaces++;
            return true;
        } else {
            add_new<Origin>(insertion);
            _stats._set_adds++;
            return false;
        }
    }

    template <typename Origin = local_origin_tag>
    bool add(item_insertion_data& insertion) {
        auto i = find(insertion.key);
        if (i != _cache.end()) {
            return false;
        }

        _stats._set_adds++;
        add_new<Origin>(insertion);
        return true;
    }

    template <typename Origin = local_origin_tag>
    bool replace(item_insertion_data& insertion) {
        auto i = find(insertion.key);
        if (i == _cache.end()) {
            return false;
        }

        _stats._set_replaces++;
        add_overriding<Origin>(i, insertion);
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
        erase(item_ref);
        return true;
    }

    item_ptr get(const item_key& key) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._get_misses++;
            return nullptr;
        }
        _stats._get_hits++;
        auto& item_ref = *i;
        return item_ptr(&item_ref);
    }

    template <typename Origin = local_origin_tag>
    cas_result cas(item_insertion_data& insertion, item::version_type version) {
        auto i = find(insertion.key);
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
        add_overriding<Origin>(i, insertion);
        return cas_result::stored;
    }

    size_t size() {
        return _cache.size();
    }

    size_t bucket_count() {
        return _cache.bucket_count();
    }

    cache_stats stats() {
        _stats._size = size();
        return _stats;
    }

    template <typename Origin = local_origin_tag>
    std::pair<item_ptr, bool> incr(item_key& key, uint64_t delta) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._incr_misses++;
            return {item_ptr{}, false};
        }
        auto& item_ref = *i;
        _stats._incr_hits++;
        auto value = item_ref.data_as_integral();
        if (!value) {
            return {boost::intrusive_ptr<item>(&item_ref), false};
        }
        item_insertion_data insertion {
            .key = Origin::move_if_local(key),
            .ascii_prefix = sstring(item_ref.ascii_prefix().data(), item_ref.ascii_prefix_size()),
            .data = to_sstring(*value + delta),
            .expiry = item_ref._expiry
        };
        i = add_overriding<local_origin_tag>(i, insertion);
        return {boost::intrusive_ptr<item>(&*i), true};
    }

    template <typename Origin = local_origin_tag>
    std::pair<item_ptr, bool> decr(item_key& key, uint64_t delta) {
        auto i = find(key);
        if (i == _cache.end()) {
            _stats._decr_misses++;
            return {item_ptr{}, false};
        }
        auto& item_ref = *i;
        _stats._decr_hits++;
        auto value = item_ref.data_as_integral();
        if (!value) {
            return {boost::intrusive_ptr<item>(&item_ref), false};
        }
        item_insertion_data insertion {
            .key = Origin::move_if_local(key),
            .ascii_prefix = sstring(item_ref.ascii_prefix().data(), item_ref.ascii_prefix_size()),
            .data = to_sstring(*value - std::min(*value, delta)),
            .expiry = item_ref._expiry
        };
        i = add_overriding<local_origin_tag>(i, insertion);
        return {boost::intrusive_ptr<item>(&*i), true};
    }

    std::pair<unsigned, foreign_ptr<lw_shared_ptr<std::string>>> print_hash_stats() {
        static constexpr unsigned bits = sizeof(size_t) * 8;
        size_t histo[bits + 1] {};
        size_t max_size = 0;
        unsigned max_bucket = 0;

        for (size_t i = 0; i < _cache.bucket_count(); i++) {
            size_t size = _cache.bucket_size(i);
            unsigned bucket;
            if (size == 0) {
                bucket = 0;
            } else {
                bucket = bits - count_leading_zeros(size);
            }
            max_bucket = std::max(max_bucket, bucket);
            max_size = std::max(max_size, size);
            histo[bucket]++;
        }

        std::stringstream ss;

        ss << "size: " << _cache.size() << "\n";
        ss << "buckets: " << _cache.bucket_count() << "\n";
        ss << "load: " << to_sstring_sprintf((double)_cache.size() / _cache.bucket_count(), "%.2lf") << "\n";
        ss << "max bucket occupancy: " << max_size << "\n";
        ss << "bucket occupancy histogram:\n";

        for (unsigned i = 0; i < (max_bucket + 2); i++) {
            ss << "  ";
            if (i == 0) {
                ss << "0: ";
            } else if (i == 1) {
                ss << "1: ";
            } else {
                ss << (1 << (i - 1)) << "+: ";
            }
            ss << histo[i] << "\n";
        }
        return {engine().cpu_id(), make_foreign(make_lw_shared<std::string>(ss.str()))};
    }

    future<> stop() { return make_ready_future<>(); }
};

class sharded_cache {
private:
    distributed<cache>& _peers;

    inline
    unsigned get_cpu(const item_key& key) {
        return std::hash<item_key>()(key) % smp::count;
    }
public:
    sharded_cache(distributed<cache>& peers) : _peers(peers) {}

    future<> flush_all() {
        return _peers.invoke_on_all(&cache::flush_all);
    }

    future<> flush_at(clock_type::time_point time_point) {
        return _peers.invoke_on_all(&cache::flush_at, time_point);
    }

    // The caller must keep @insertion live until the resulting future resolves.
    future<bool> set(item_insertion_data& insertion) {
        auto cpu = get_cpu(insertion.key);
        if (engine().cpu_id() == cpu) {
            return make_ready_future<bool>(_peers.local().set(insertion));
        }
        return _peers.invoke_on(cpu, &cache::set<remote_origin_tag>, std::ref(insertion));
    }

    // The caller must keep @insertion live until the resulting future resolves.
    future<bool> add(item_insertion_data& insertion) {
        auto cpu = get_cpu(insertion.key);
        if (engine().cpu_id() == cpu) {
            return make_ready_future<bool>(_peers.local().add(insertion));
        }
        return _peers.invoke_on(cpu, &cache::add<remote_origin_tag>, std::ref(insertion));
    }

    // The caller must keep @insertion live until the resulting future resolves.
    future<bool> replace(item_insertion_data& insertion) {
        auto cpu = get_cpu(insertion.key);
        if (engine().cpu_id() == cpu) {
            return make_ready_future<bool>(_peers.local().replace(insertion));
        }
        return _peers.invoke_on(cpu, &cache::replace<remote_origin_tag>, std::ref(insertion));
    }

    // The caller must keep @key live until the resulting future resolves.
    future<bool> remove(const item_key& key) {
        auto cpu = get_cpu(key);
        return _peers.invoke_on(cpu, &cache::remove, std::ref(key));
    }

    // The caller must keep @key live until the resulting future resolves.
    future<item_ptr> get(const item_key& key) {
        auto cpu = get_cpu(key);
        return _peers.invoke_on(cpu, &cache::get, std::ref(key));
    }

    // The caller must keep @insertion live until the resulting future resolves.
    future<cas_result> cas(item_insertion_data& insertion, item::version_type version) {
        auto cpu = get_cpu(insertion.key);
        if (engine().cpu_id() == cpu) {
            return make_ready_future<cas_result>(_peers.local().cas(insertion, version));
        }
        return _peers.invoke_on(cpu, &cache::cas<remote_origin_tag>, std::ref(insertion), std::move(version));
    }

    future<cache_stats> stats() {
        return _peers.map_reduce(adder<cache_stats>(), &cache::stats);
    }

    // The caller must keep @key live until the resulting future resolves.
    future<std::pair<item_ptr, bool>> incr(item_key& key, uint64_t delta) {
        auto cpu = get_cpu(key);
        if (engine().cpu_id() == cpu) {
            return make_ready_future<std::pair<item_ptr, bool>>(
                _peers.local().incr<local_origin_tag>(key, delta));
        }
        return _peers.invoke_on(cpu, &cache::incr<remote_origin_tag>, std::ref(key), std::move(delta));
    }

    // The caller must keep @key live until the resulting future resolves.
    future<std::pair<item_ptr, bool>> decr(item_key& key, uint64_t delta) {
        auto cpu = get_cpu(key);
        if (engine().cpu_id() == cpu) {
            return make_ready_future<std::pair<item_ptr, bool>>(
                _peers.local().decr(key, delta));
        }
        return _peers.invoke_on(cpu, &cache::decr<remote_origin_tag>, std::ref(key), std::move(delta));
    }

    future<> print_hash_stats(output_stream<char>& out) {
        return _peers.map_reduce([&out] (std::pair<unsigned, foreign_ptr<lw_shared_ptr<std::string>>> data) mutable {
            return out.write("=== CPU " + std::to_string(data.first) + " ===\r\n")
                .then([&out, str = std::move(data.second)] {
                    return out.write(*str);
                });
            }, &cache::print_hash_stats);
    }
};

struct system_stats {
    uint32_t _curr_connections {};
    uint32_t _total_connections {};
    uint64_t _cmd_get {};
    uint64_t _cmd_set {};
    uint64_t _cmd_flush {};
    clock_type::time_point _start_time;
public:
    system_stats() {
        _start_time = clock_type::time_point::max();
    }
    system_stats(clock_type::time_point start_time)
        : _start_time(start_time) {
    }
    system_stats self() {
        return *this;
    }
    void operator+=(const system_stats& other) {
        _curr_connections += other._curr_connections;
        _total_connections += other._total_connections;
        _cmd_get += other._cmd_get;
        _cmd_set += other._cmd_set;
        _cmd_flush += other._cmd_flush;
        _start_time = std::min(_start_time, other._start_time);
    }
    future<> stop() { return make_ready_future<>(); }
};

class ascii_protocol {
private:
    using this_type = ascii_protocol;
    sharded_cache& _cache;
    distributed<system_stats>& _system_stats;
    memcache_ascii_parser _parser;
    item_key _item_key;
    item_insertion_data _insertion;
    std::vector<item_ptr> _items;
private:
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
    static constexpr const char *msg_out_of_memory = "SERVER_ERROR Out of memory allocating new item\r\n";
    static constexpr const char *msg_error_non_numeric_value = "CLIENT_ERROR cannot increment or decrement non-numeric value\r\n";
private:
    template <bool WithVersion>
    static void append_item(scattered_message<char>& msg, item_ptr item) {
        if (!item) {
            return;
        }

        msg.append_static("VALUE ");
        msg.append_static(item->key());
        msg.append_static(item->ascii_prefix());

        if (WithVersion) {
             msg.append_static(" ");
             msg.append(to_sstring(item->version()));
        }

        msg.append_static(msg_crlf);
        msg.append_static(item->value());
        msg.append_static(msg_crlf);
        msg.on_delete([item = std::move(item)] {});
    }

    template <bool WithVersion>
    future<> handle_get(output_stream<char>& out) {
        _system_stats.local()._cmd_get++;
        if (_parser._keys.size() == 1) {
            return _cache.get(_parser._keys[0]).then([&out] (auto item) -> future<> {
                scattered_message<char> msg;
                this_type::append_item<WithVersion>(msg, std::move(item));
                msg.append_static(msg_end);
                return out.write(std::move(msg));
            });
        } else {
            _items.clear();
            return parallel_for_each(_parser._keys.begin(), _parser._keys.end(), [this] (const auto& key) {
                return _cache.get(key).then([this] (auto item) {
                    _items.emplace_back(std::move(item));
                });
            }).then([this, &out] () {
                scattered_message<char> msg;
                for (auto& item : _items) {
                    append_item<WithVersion>(msg, std::move(item));
                }
                msg.append_static(msg_end);
                return out.write(std::move(msg));
            });
        }
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
        return _cache.stats().then([this, &out] (auto stats) {
            return _system_stats.map_reduce(adder<system_stats>(), &system_stats::self)
                .then([this, &out, all_cache_stats = std::move(stats)] (auto all_system_stats) -> future<> {
                    auto now = clock_type::now();
                    auto total_items = all_cache_stats._set_replaces + all_cache_stats._set_adds
                        + all_cache_stats._cas_hits;
                    return print_stat(out, "pid", getpid())
                        .then([this, now, &out, uptime = now - all_system_stats._start_time] {
                            return print_stat(out, "uptime",
                                std::chrono::duration_cast<std::chrono::seconds>(uptime).count());
                        }).then([this, now, &out] {
                            return print_stat(out, "time",
                                std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
                        }).then([this, &out] {
                            return print_stat(out, "version", VERSION_STRING);
                        }).then([this, &out] {
                            return print_stat(out, "pointer_size", sizeof(void*)*8);
                        }).then([this, &out, v = all_system_stats._curr_connections] {
                            return print_stat(out, "curr_connections", v);
                        }).then([this, &out, v = all_system_stats._total_connections] {
                            return print_stat(out, "total_connections", v);
                        }).then([this, &out, v = all_system_stats._curr_connections] {
                            return print_stat(out, "connection_structures", v);
                        }).then([this, &out, v = all_system_stats._cmd_get] {
                            return print_stat(out, "cmd_get", v);
                        }).then([this, &out, v = all_system_stats._cmd_set] {
                            return print_stat(out, "cmd_set", v);
                        }).then([this, &out, v = all_system_stats._cmd_flush] {
                            return print_stat(out, "cmd_flush", v);
                        }).then([this, &out] {
                            return print_stat(out, "cmd_touch", 0);
                        }).then([this, &out, v = all_cache_stats._get_hits] {
                            return print_stat(out, "get_hits", v);
                        }).then([this, &out, v = all_cache_stats._get_misses] {
                            return print_stat(out, "get_misses", v);
                        }).then([this, &out, v = all_cache_stats._delete_misses] {
                            return print_stat(out, "delete_misses", v);
                        }).then([this, &out, v = all_cache_stats._delete_hits] {
                            return print_stat(out, "delete_hits", v);
                        }).then([this, &out, v = all_cache_stats._incr_misses] {
                            return print_stat(out, "incr_misses", v);
                        }).then([this, &out, v = all_cache_stats._incr_hits] {
                            return print_stat(out, "incr_hits", v);
                        }).then([this, &out, v = all_cache_stats._decr_misses] {
                            return print_stat(out, "decr_misses", v);
                        }).then([this, &out, v = all_cache_stats._decr_hits] {
                            return print_stat(out, "decr_hits", v);
                        }).then([this, &out, v = all_cache_stats._cas_misses] {
                            return print_stat(out, "cas_misses", v);
                        }).then([this, &out, v = all_cache_stats._cas_hits] {
                            return print_stat(out, "cas_hits", v);
                        }).then([this, &out, v = all_cache_stats._cas_badval] {
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
                        }).then([this, &out, v = all_cache_stats._size] {
                            return print_stat(out, "curr_items", v);
                        }).then([this, &out, v = total_items] {
                            return print_stat(out, "total_items", v);
                        }).then([this, &out, v = all_cache_stats._expired] {
                            return print_stat(out, "seastar.expired", v);
                        }).then([this, &out, v = all_cache_stats._resize_failure] {
                            return print_stat(out, "seastar.resize_failure", v);
                        }).then([this, &out, v = all_cache_stats._evicted] {
                            return print_stat(out, "evictions", v);
                        }).then([this, &out, v = all_cache_stats._bytes] {
                            return print_stat(out, "bytes", v);
                        }).then([&out] {
                            return out.write(msg_end);
                        });
                });
        });
    }
public:
    ascii_protocol(sharded_cache& cache, distributed<system_stats>& system_stats)
        : _cache(cache)
        , _system_stats(system_stats)
    {}

    void prepare_insertion() {
        _insertion = item_insertion_data{
            .key = std::move(_parser._key),
            .ascii_prefix = make_sstring(" ", _parser._flags_str, " ", _parser._size_str),
            .data = std::move(_parser._blob),
            .expiry = expiration(_parser._expiration)
        };
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
                {
                    _system_stats.local()._cmd_set++;
                    prepare_insertion();
                    auto f = _cache.set(_insertion);
                    if (_parser._noreply) {
                        return std::move(f).discard_result();
                    }
                    return std::move(f).then([&out] (...) {
                        return out.write(msg_stored);
                    });
                }

                case memcache_ascii_parser::state::cmd_cas:
                {
                    _system_stats.local()._cmd_set++;
                    prepare_insertion();
                    auto f = _cache.cas(_insertion, _parser._version);
                    if (_parser._noreply) {
                        return std::move(f).discard_result();
                    }
                    return std::move(f).then([&out] (auto result) {
                        switch (result) {
                            case cas_result::stored:
                                return out.write(msg_stored);
                            case cas_result::not_found:
                                return out.write(msg_not_found);
                            case cas_result::bad_version:
                                return out.write(msg_exists);
                            default:
                                std::abort();
                        }
                    });
                }

                case memcache_ascii_parser::state::cmd_add:
                {
                    _system_stats.local()._cmd_set++;
                    prepare_insertion();
                    auto f = _cache.add(_insertion);
                    if (_parser._noreply) {
                        return std::move(f).discard_result();
                    }
                    return std::move(f).then([&out] (bool added) {
                        return out.write(added ? msg_stored : msg_not_stored);
                    });
                }

                case memcache_ascii_parser::state::cmd_replace:
                {
                    _system_stats.local()._cmd_set++;
                    prepare_insertion();
                    auto f = _cache.replace(_insertion);
                    if (_parser._noreply) {
                        return std::move(f).discard_result();
                    }
                    return std::move(f).then([&out] (auto replaced) {
                        return out.write(replaced ? msg_stored : msg_not_stored);
                    });
                }

                case memcache_ascii_parser::state::cmd_get:
                    return handle_get<false>(out);

                case memcache_ascii_parser::state::cmd_gets:
                    return handle_get<true>(out);

                case memcache_ascii_parser::state::cmd_delete:
                {
                    auto f = _cache.remove(_parser._key);
                    if (_parser._noreply) {
                        return std::move(f).discard_result();
                    }
                    return std::move(f).then([&out] (bool removed) {
                        return out.write(removed ? msg_deleted : msg_not_found);
                    });
                }

                case memcache_ascii_parser::state::cmd_flush_all:
                {
                    _system_stats.local()._cmd_flush++;
                    if (_parser._expiration) {
                        auto expiry = expiration(_parser._expiration);
                        auto f = _cache.flush_at(expiry.to_time_point());
                        if (_parser._noreply) {
                            return f;
                        }
                        return std::move(f).then([&out] {
                            return out.write(msg_ok);
                        });
                    } else {
                        auto f = _cache.flush_all();
                        if (_parser._noreply) {
                            return f;
                        }
                        return std::move(f).then([&out] {
                            return out.write(msg_ok);
                        });
                    }
                }

                case memcache_ascii_parser::state::cmd_version:
                    return out.write(msg_version);

                case memcache_ascii_parser::state::cmd_stats:
                    return print_stats(out);

                case memcache_ascii_parser::state::cmd_stats_hash:
                    return _cache.print_hash_stats(out);

                case memcache_ascii_parser::state::cmd_incr:
                {
                    auto f = _cache.incr(_parser._key, _parser._u64);
                    if (_parser._noreply) {
                        return std::move(f).discard_result();
                    }
                    return std::move(f).then([&out] (auto result) {
                        auto item = std::move(result.first);
                        if (!item) {
                            return out.write(msg_not_found);
                        }
                        auto incremented = result.second;
                        if (!incremented) {
                            return out.write(msg_error_non_numeric_value);
                        }
                        return out.write(item->value().data(), item->value_size()).then([&out] {
                            return out.write(msg_crlf);
                        });
                    });
                }

                case memcache_ascii_parser::state::cmd_decr:
                {
                    auto f = _cache.decr(_parser._key, _parser._u64);
                    if (_parser._noreply) {
                        return std::move(f).discard_result();
                    }
                    return std::move(f).then([&out] (auto result) {
                        auto item = std::move(result.first);
                        if (!item) {
                            return out.write(msg_not_found);
                        }
                        auto decremented = result.second;
                        if (!decremented) {
                            return out.write(msg_error_non_numeric_value);
                        }
                        return out.write(item->value().data(), item->value_size()).then([&out] {
                            return out.write(msg_crlf);
                        });
                    });
                }
            };
            std::abort();
        }).then_wrapped([this, &out] (auto&& f) -> future<> {
            // FIXME: then_wrapped() being scheduled even though no exception was triggered has a
            // performance cost of about 2.6%. Not using it means maintainability penalty.
            try {
                f.get();
            } catch (std::bad_alloc& e) {
                if (_parser._noreply) {
                    return make_ready_future<>();
                }
                return out.write(msg_out_of_memory);
            }
            return make_ready_future<>();
        });
    };
};

class udp_server {
public:
    static const size_t default_max_datagram_size = 1400;
private:
    sharded_cache& _cache;
    distributed<system_stats>& _system_stats;
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

    struct connection {
        ipv4_addr _src;
        uint16_t _request_id;
        input_stream<char> _in;
        output_stream<char> _out;
        std::vector<packet> _out_bufs;
        ascii_protocol _proto;

        connection(ipv4_addr src, uint16_t request_id, input_stream<char>&& in, size_t out_size,
                sharded_cache& c, distributed<system_stats>& system_stats)
            : _src(src)
            , _request_id(request_id)
            , _in(std::move(in))
            , _out(output_stream<char>(data_sink(std::make_unique<vector_data_sink>(_out_bufs)), out_size, true))
            , _proto(c, system_stats)
        {}

        future<> respond(udp_channel& chan) {
            int i = 0;
            return do_for_each(_out_bufs.begin(), _out_bufs.end(), [this, i, &chan] (packet& p) mutable {
                header* out_hdr = p.prepend_header<header>(0);
                out_hdr->_request_id = _request_id;
                out_hdr->_sequence_number = i++;
                out_hdr->_n = _out_bufs.size();
                *out_hdr = hton(*out_hdr);
                return chan.send(_src, std::move(p));
            });
        }
    };

public:
    udp_server(sharded_cache& c, distributed<system_stats>& system_stats, uint16_t port = 11211)
         : _cache(c)
         , _system_stats(system_stats)
         , _port(port)
    {}

    void set_max_datagram_size(size_t max_datagram_size) {
        _max_datagram_size = max_datagram_size;
    }

    void start() {
        _chan = engine().net().make_udp_channel({_port});
        keep_doing([this] {
            return _chan.receive().then([this](udp_datagram dgram) {
                packet& p = dgram.get_data();
                if (p.len() < sizeof(header)) {
                    // dropping invalid packet
                    return make_ready_future<>();
                }

                header hdr = ntoh(*p.get_header<header>());
                p.trim_front(sizeof(hdr));

                auto request_id = hdr._request_id;
                auto in = as_input_stream(std::move(p));
                auto conn = make_lw_shared<connection>(dgram.get_src(), request_id, std::move(in),
                    _max_datagram_size - sizeof(header), _cache, _system_stats);

                if (hdr._n != 1 || hdr._sequence_number != 0) {
                    return conn->_out.write("CLIENT_ERROR only single-datagram requests supported\r\n").then([this, conn] {
                        return conn->_out.flush().then([this, conn] {
                            return conn->respond(_chan).then([conn] {});
                        });
                    });
                }

                return conn->_proto.handle(conn->_in, conn->_out).then([this, conn]() mutable {
                    return conn->_out.flush().then([this, conn] {
                        return conn->respond(_chan).then([conn] {});
                    });
                });
            });
        }).or_terminate();
    };

    future<> stop() { return make_ready_future<>(); }
};

class tcp_server {
private:
    lw_shared_ptr<server_socket> _listener;
    sharded_cache& _cache;
    distributed<system_stats>& _system_stats;
    uint16_t _port;
    struct connection {
        connected_socket _socket;
        socket_address _addr;
        input_stream<char> _in;
        output_stream<char> _out;
        ascii_protocol _proto;
        distributed<system_stats>& _system_stats;
        connection(connected_socket&& socket, socket_address addr, sharded_cache& c, distributed<system_stats>& system_stats)
            : _socket(std::move(socket))
            , _addr(addr)
            , _in(_socket.input())
            , _out(_socket.output())
            , _proto(c, system_stats)
            , _system_stats(system_stats)
        {
            _system_stats.local()._curr_connections++;
            _system_stats.local()._total_connections++;
        }
        ~connection() {
            _system_stats.local()._curr_connections--;
        }
    };
public:
    tcp_server(sharded_cache& cache, distributed<system_stats>& system_stats, uint16_t port = 11211)
        : _cache(cache)
        , _system_stats(system_stats)
        , _port(port)
    {}

    void start() {
        listen_options lo;
        lo.reuse_address = true;
        _listener = engine().listen(make_ipv4_address({_port}), lo);
        keep_doing([this] {
            return _listener->accept().then([this] (connected_socket fd, socket_address addr) mutable {
                auto conn = make_lw_shared<connection>(std::move(fd), addr, _cache, _system_stats);
                do_until([conn] { return conn->_in.eof(); }, [this, conn] {
                    return conn->_proto.handle(conn->_in, conn->_out).then([conn] {
                        return conn->_out.flush();
                    });
                });
            });
        }).or_terminate();
    }

    future<> stop() { return make_ready_future<>(); }
};

class stats_printer {
private:
    timer<> _timer;
    sharded_cache& _cache;
public:
    stats_printer(sharded_cache& cache)
        : _cache(cache) {}

    void start() {
        _timer.set_callback([this] {
            _cache.stats().then([this] (auto stats) {
                auto gets_total = stats._get_hits + stats._get_misses;
                auto get_hit_rate = gets_total ? ((double)stats._get_hits * 100 / gets_total) : 0;
                auto sets_total = stats._set_adds + stats._set_replaces;
                auto set_replace_rate = sets_total ? ((double)stats._set_replaces * 100/ sets_total) : 0;
                std::cout << "items: " << stats._size << " "
                    << std::setprecision(2) << std::fixed
                    << "get: " << stats._get_hits << "/" << gets_total << " (" << get_hit_rate << "%) "
                    << "set: " << stats._set_replaces << "/" << sets_total << " (" <<  set_replace_rate << "%)";
                std::cout << std::endl;
            });
        });
        _timer.arm_periodic(std::chrono::seconds(1));
    }

    future<> stop() { return make_ready_future<>(); }
};

} /* namespace memcache */

int main(int ac, char** av) {
    distributed<memcache::cache> cache_peers;
    memcache::sharded_cache cache(cache_peers);
    distributed<memcache::system_stats> system_stats;
    distributed<memcache::udp_server> udp_server;
    distributed<memcache::tcp_server> tcp_server;
    memcache::stats_printer stats(cache);

    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("max-datagram-size", bpo::value<int>()->default_value(memcache::udp_server::default_max_datagram_size),
             "Maximum size of UDP datagram")
        ("max-slab-size", bpo::value<uint64_t>()->default_value(memcache::default_per_cpu_slab_size/MB),
             "Maximum memory to be used for items (value in megabytes) (reclaimer is disabled if set)")
        ("slab-page-size", bpo::value<uint64_t>()->default_value(memcache::default_slab_page_size/MB),
             "Size of slab page (value in megabytes)")
        ("stats",
             "Print basic statistics periodically (every second)")
        ("port", bpo::value<uint16_t>()->default_value(11211),
             "Specify UDP and TCP ports for memcached server to listen on")
        ;

    return app.run(ac, av, [&] {
        engine().at_exit([&] { return tcp_server.stop(); });
        engine().at_exit([&] { return udp_server.stop(); });
        engine().at_exit([&] { return cache_peers.stop(); });
        engine().at_exit([&] { return system_stats.stop(); });

        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        uint64_t per_cpu_slab_size = config["max-slab-size"].as<uint64_t>() * MB;
        uint64_t slab_page_size = config["slab-page-size"].as<uint64_t>() * MB;
        return cache_peers.start(std::move(per_cpu_slab_size), std::move(slab_page_size)).then([&system_stats] {
            return system_stats.start(clock_type::now());
        }).then([&] {
            std::cout << PLATFORM << " memcached " << VERSION << "\n";
            return make_ready_future<>();
        }).then([&, port] {
            return tcp_server.start(std::ref(cache), std::ref(system_stats), port);
        }).then([&tcp_server] {
            return tcp_server.invoke_on_all(&memcache::tcp_server::start);
        }).then([&, port] {
            if (engine().net().has_per_core_namespace()) {
                return udp_server.start(std::ref(cache), std::ref(system_stats), port);
            } else {
                return udp_server.start_single(std::ref(cache), std::ref(system_stats), port);
            }
        }).then([&] {
            return udp_server.invoke_on_all(&memcache::udp_server::set_max_datagram_size,
                    (size_t)config["max-datagram-size"].as<int>());
        }).then([&] {
            return udp_server.invoke_on_all(&memcache::udp_server::start);
        }).then([&stats, start_stats = config.count("stats")] {
            if (start_stats) {
                stats.start();
            }
        });
    });
}
