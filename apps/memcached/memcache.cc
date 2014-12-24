/*
 * Copyright 2014 Cloudius Systems
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
#include "core/smp.hh"
#include "core/vector-data-sink.hh"
#include "net/api.hh"
#include "net/packet-data-source.hh"
#include "apps/memcached/ascii.hh"
#include "core/bitops.hh"
#include "memcached.hh"
#include <unistd.h>
#include <queue>

#define PLATFORM "seastar"
#define VERSION "v1.0"
#define VERSION_STRING PLATFORM " " VERSION

using namespace net;

namespace bi = boost::intrusive;


namespace flashcache {

constexpr int block_size_shift = 12;
constexpr uint32_t block_size = 1 << block_size_shift;

struct block {
private:
    uint32_t _blk_id; // granularity: block_size
public:
    block() = default;
    block(uint32_t blk_id) : _blk_id(blk_id) {}
    uint64_t get_addr() { return _blk_id * block_size; }
};

struct devfile {
private:
    file _f;
public:
    devfile(file&& f) : _f(std::move(f)) {}

    file& f() {
        return _f;
    }

    friend class subdevice;
};

class subdevice {
    foreign_ptr<shared_ptr<flashcache::devfile>> _dev;
    uint64_t _offset;
    uint64_t _end;
    std::queue<block> _free_blocks;
    semaphore _par = { 1000 };
public:
    subdevice(foreign_ptr<shared_ptr<flashcache::devfile>> dev, uint64_t offset, uint64_t length)
        : _dev(std::move(dev))
        , _offset(offset)
        , _end(offset + length)
    {
        auto blks = length / block_size;
        for (auto blk_id = 0U; blk_id < blks; blk_id++) {
            _free_blocks.push(blk_id);
        }
    }

    block allocate(void) {
        // FIXME: handle better the case where there is no disk space left for allocations.
        assert(!_free_blocks.empty());
        block blk = _free_blocks.front();
        _free_blocks.pop();
        return blk;
    }

    void free(block blk) {
        auto actual_blk_addr = _offset + blk.get_addr();
        assert(actual_blk_addr + block_size <= _end);
        // Issue trimming operation on the block being freed.
        _dev->_f.discard(actual_blk_addr, block_size).finally([this, blk]() mutable {
            _free_blocks.push(blk);
        });
    }

    future<size_t> read(block& blk, void* buffer) {
        auto actual_blk_addr = _offset + blk.get_addr();
        assert(actual_blk_addr + block_size <= _end);
        return _dev->_f.dma_read(actual_blk_addr, buffer, block_size);
    }

    future<size_t> write(block& blk, const void* buffer) {
        auto actual_blk_addr = _offset + blk.get_addr();
        assert(actual_blk_addr + block_size <= _end);
        return _dev->_f.dma_write(actual_blk_addr, buffer, block_size);
    }

    future<> wait() {
        return _par.wait();
    }

    void signal() {
        _par.signal();
    }
};

} /* namespace flashcache */

namespace memcache {

template<typename T>
using optional = boost::optional<T>;


struct memcache_item_base {
    memcache_item_base(uint32_t size) {}
};

enum class item_state {
    MEM,
    TO_MEM_DISK, // transition period from MEM to MEM_DISK
    MEM_DISK,
    DISK,
    ERASED,
};

struct flashcache_item_base {
private:
    item_state _state = item_state::MEM;
    uint32_t _size;
    // NOTE: vector must be sorted, i.e. first block of data should be in the front of the list.
    std::vector<flashcache::block> _used_blocks;
    flashcache::subdevice* _subdev = nullptr;
public:
    semaphore _lookup_sem = { 1 };

    flashcache_item_base(uint32_t size) : _size(size) {}

    ~flashcache_item_base() {
        if (_used_blocks.empty()) {
            return;
        }
        assert(_subdev != nullptr);
        // Needed to free used blocks only when the underlying item is destroyed,
        // otherwise they could be reused while there is I/O in progress to them.
        for (auto& blk : _used_blocks) {
            _subdev->free(blk);
        }
        _used_blocks.clear();
    }

    void set_subdevice(flashcache::subdevice* subdev) {
        assert(_subdev == nullptr);
        _subdev = subdev;
    }

    bool is_present() {
        return (_state == item_state::MEM || _state == item_state::TO_MEM_DISK ||
            _state == item_state::MEM_DISK);
    }

    item_state get_state() {
        return _state;
    }

    void set_state(item_state state) {
        _state = state;
    }

    uint32_t size() {
        return _size;
    }

    size_t used_blocks_size() {
        return _used_blocks.size();
    }

    void used_blocks_clear() {
        _used_blocks.clear();
    }

    bool used_blocks_empty() {
        return _used_blocks.empty();
    }

    void used_blocks_resize(size_t new_size) {
        _used_blocks.resize(new_size);
    }

    flashcache::block used_block(unsigned int index) {
        assert(index < _used_blocks.size());
        return _used_blocks[index];
    }

    void use_block(unsigned int index, flashcache::block blk) {
        assert(index < _used_blocks.size());
        _used_blocks[index] = blk;
    }
};

template <bool WithFlashCache>
class item : public std::conditional<WithFlashCache, flashcache_item_base, memcache_item_base>::type {
public:
    using item_type = item<WithFlashCache>;
    using version_type = uint64_t;
    using time_point = clock_type::time_point;
    using duration = clock_type::duration;
private:
    using hook_type = bi::unordered_set_member_hook<>;
    // TODO: align shared data to cache line boundary
    item_key _key;
    sstring _data;
    const sstring _ascii_prefix;
    version_type _version;
    int _ref_count;
    hook_type _cache_link;
    bi::list_member_hook<> _lru_link;
    bi::list_member_hook<> _timer_link;
    time_point _expiry;
    template <bool>
    friend class cache;
    friend class memcache_cache_base;
    friend class flashcache_cache_base;
public:
    item(item_key&& key, sstring&& ascii_prefix, sstring&& data, clock_type::time_point expiry, version_type version = 1)
        : std::conditional<WithFlashCache, flashcache_item_base, memcache_item_base>::type(data.size())
        , _key(std::move(key))
        , _data(std::move(data))
        , _ascii_prefix(std::move(ascii_prefix))
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

    sstring& data() {
        return _data;
    }

    const sstring& ascii_prefix() {
        return _ascii_prefix;
    }

    const sstring& key() {
        return _key.key();
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

    // needed by timer_set
    bool cancel() {
        return false;
    }

    friend bool operator==(const item_type &a, const item_type &b) {
         return a._key == b._key;
    }

    friend std::size_t hash_value(const item_type &i) {
        return std::hash<item_key>()(i._key);
    }

    friend inline void intrusive_ptr_add_ref(item_type* it) {
        ++it->_ref_count;
    }

    friend inline void intrusive_ptr_release(item_type* it) {
        if (--it->_ref_count == 0) {
            delete it;
        }
    }

    template <bool>
    friend class item_key_cmp;
};

template <bool WithFlashCache>
struct item_key_cmp
{
    bool operator()(const item_key& key, const item<WithFlashCache>& it) const {
        return key == it._key;
    }

    bool operator()(const item<WithFlashCache>& it, const item_key& key) const {
        return key == it._key;
    }
};

template <bool WithFlashCache>
using item_ptr = foreign_ptr<boost::intrusive_ptr<item<WithFlashCache>>>;

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
    // flashcache-only stats.
    size_t _loads{};
    size_t _stores{};

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
        _loads += o._loads;
        _stores += o._stores;
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
    clock_type::time_point expiry;
};

struct memcache_cache_base {
private:
    using item_type = item<false>;
    using item_lru_list = bi::list<item_type,
        bi::member_hook<item_type,
        bi::list_member_hook<>, &item_type::_lru_link>>;
    item_lru_list _lru;
    cache_stats _stats;
public:
    void do_setup(foreign_ptr<shared_ptr<flashcache::devfile>> dev, uint64_t offset, uint64_t length) {}

    void do_erase(item_type& item_ref) {
        _lru.erase(_lru.iterator_to(item_ref));
    }

    size_t do_reclaim(size_t target) {
        return 0;
    }

    future<> do_get(boost::intrusive_ptr<item_type> item) {
        auto& item_ref = *item;
        _lru.erase(_lru.iterator_to(item_ref));
        _lru.push_front(item_ref);
        return make_ready_future<>();
    }

    void do_set(item_type& new_item_ref) {}

    template <bool>
    friend class cache;
};

struct flashcache_cache_base {
private:
    using item_type = item<true>;
    using item_lru_list = bi::list<item_type,
        bi::member_hook<item_type,
        bi::list_member_hook<>, &item_type::_lru_link>>;
    item_lru_list _lru; // mem_lru
    item_lru_list _mem_disk_lru;
    item_lru_list _disk_lru;
    uint64_t _total_mem = 0; // total bytes from items' value in mem lru.
    uint64_t _total_mem_disk = 0; // total bytes from items' value in mem_disk lru.
    std::unique_ptr<flashcache::subdevice> _subdev;
    cache_stats _stats;

    future<> load_item_data(boost::intrusive_ptr<item_type> item);
    future<> store_item_data(boost::intrusive_ptr<item_type> item);
public:
    flashcache::subdevice& get_subdevice() {
        auto& subdev_ref = *_subdev.get();
        return subdev_ref;
    }

    void do_setup(foreign_ptr<shared_ptr<flashcache::devfile>> dev, uint64_t offset, uint64_t length) {
        _subdev = std::make_unique<flashcache::subdevice>(std::move(dev), offset, length);
    }

    void do_erase(item_type& item_ref) {
        switch(item_ref.get_state()) {
        case item_state::MEM:
            _lru.erase(_lru.iterator_to(item_ref));
            _total_mem -= item_ref.size();
            break;
        case item_state::TO_MEM_DISK:
            _total_mem_disk -= item_ref.size();
            break;
        case item_state::MEM_DISK:
            _mem_disk_lru.erase(_mem_disk_lru.iterator_to(item_ref));
            _total_mem_disk -= item_ref.size();
            break;
        case item_state::DISK:
            _disk_lru.erase(_disk_lru.iterator_to(item_ref));
            break;
        default:
            assert(0);
        }
        item_ref.set_state(item_state::ERASED);
    }

    size_t do_reclaim(size_t target) {
        size_t reclaimed_so_far = 0;

        auto i = this->_mem_disk_lru.end();
        if (i == this->_mem_disk_lru.begin()) {
            return 0;
        }

        --i;

        bool done = false;
        do {
            item_type& victim = *i;
            if (i != this->_mem_disk_lru.begin()) {
                --i;
            } else {
                done = true;
            }

            if (victim._ref_count == 1) {
                auto item_data_size = victim.size();

                assert(victim.data().size() == item_data_size);
                _mem_disk_lru.erase(_mem_disk_lru.iterator_to(victim));
                victim.data().reset();
                assert(victim.data().size() == 0);
                victim.set_state(item_state::DISK);
                _disk_lru.push_front(victim);
                reclaimed_so_far += item_data_size;
                _total_mem_disk -= item_data_size;

                if (reclaimed_so_far >= target) {
                    done = true;
                }
            }
        } while (!done);
        return reclaimed_so_far;
    }

    future<> do_get(boost::intrusive_ptr<item_type> item) {
        return load_item_data(item);
    }

    // TODO: Handle storing/loading of zero-length items.
    void do_set(item_type& new_item_ref) {
        _total_mem += new_item_ref.size();
        new_item_ref.set_subdevice(_subdev.get());

        // Adjust items between mem (20%) and mem_disk (80%) lru lists.
        // With that ratio, items will be constantly scheduled to be stored on disk,
        // and that's good because upon memory pressure, we would have enough items
        // to satisfy the amount of memory asked to be reclaimed.
        if (_total_mem >= 1*MB) {
            auto total = _total_mem + _total_mem_disk;
            auto total_mem_disk_perc = _total_mem_disk * 100 / total;
            if (total_mem_disk_perc < 80) {
                // Store least recently used item from lru into mem_disk lru.
                item_type& item_ref = _lru.back();
                auto item = boost::intrusive_ptr<item_type>(&item_ref);
                auto item_data_size = item->size();

                assert(item->get_state() == item_state::MEM);
                _lru.erase(_lru.iterator_to(item_ref));
                item->set_state(item_state::TO_MEM_DISK);
                store_item_data(item);
                _total_mem -= item_data_size;
                _total_mem_disk += item_data_size;
            }
        }
    }

    template <bool>
    friend class cache;
};

//
// Load item data from disk into memory.
// NOTE: blocks used aren't freed because item will be moved to _mem_disk_lru.
//
future<> flashcache_cache_base::load_item_data(boost::intrusive_ptr<item_type> item) {
    if (item->is_present()) {
        auto& item_ref = *item;
        switch(item->get_state()) {
        case item_state::MEM:
            _lru.erase(_lru.iterator_to(item_ref));
            _lru.push_front(item_ref);
            break;
        case item_state::TO_MEM_DISK:
            break;
        case item_state::MEM_DISK:
            _mem_disk_lru.erase(_mem_disk_lru.iterator_to(item_ref));
            _mem_disk_lru.push_front(item_ref);
            break;
        default:
            assert(0);
        }
        return make_ready_future<>();
    }
    return item->_lookup_sem.wait().then([this, item] {
        if (item->is_present()) {
            return make_ready_future<>();
        }
        assert(item->get_state() == item_state::DISK);

        flashcache::subdevice& subdev = this->get_subdevice();
        auto sem = make_shared<semaphore>({ 0 });
        auto& item_data = item->data();
        auto item_size = item->size();
        auto blocks_to_load = item->used_blocks_size();
        assert(item_data.empty());
        assert(item_size >= 1);
        assert(blocks_to_load == (item_size + (flashcache::block_size - 1)) / flashcache::block_size);

        auto to_read = item_size;
        item_data = sstring(sstring::initialized_later(), item_size);
        for (auto i = 0U; i < blocks_to_load; ++i) {
            auto read_size = std::min(to_read, flashcache::block_size);

            subdev.wait().then([&subdev, sem, item, read_size, i] {
                // If the item is already erased no need to schedule new IOs, just signal the semaphores.
                if (item->get_state() == item_state::ERASED) {
                    return make_ready_future<>();
                }
                // TODO: Avoid allocation and copying by directly using item's data (should be aligned).
                auto rbuf = allocate_aligned_buffer<unsigned char>(flashcache::block_size, flashcache::block_size);
                auto rb = rbuf.get();
                flashcache::block blk = item->used_block(i);

                return subdev.read(blk, rb).then(
                        [item, read_size, rbuf = std::move(rbuf), i] (size_t ret) mutable {
                    assert(ret == flashcache::block_size);
                    char *data = item->data().begin();
                    assert(data != nullptr);
                    assert((i * flashcache::block_size + read_size) <= item->data().size()); // overflow check
                    memcpy(data + (i * flashcache::block_size), rbuf.get(), read_size);
                }).or_terminate();
            }).finally([&subdev, sem] {
                subdev.signal();
                sem->signal(1);
            });
            to_read -= read_size;
        }

        return sem->wait(blocks_to_load).then([this, item] () mutable {
            auto& item_data = item->data();
            auto item_data_size = item_data.size();
            assert(item_data_size == item->size());

            if (item->get_state() != item_state::ERASED) {
                // Adjusting LRU: item is moved from _disk_lru to _mem_disk_lru.
                auto& item_ref = *item;
                _disk_lru.erase(_disk_lru.iterator_to(item_ref));
                item->set_state(item_state::MEM_DISK);
                _mem_disk_lru.push_front(item_ref);
                _total_mem_disk += item_data_size;
            }
            this->_stats._loads++;
        });
    }).finally([item] {
        item->_lookup_sem.signal();
    });
}

//
// Store item data from memory into disk.
// NOTE: Item data remains present in memory.
//
future<> flashcache_cache_base::store_item_data(boost::intrusive_ptr<item_type> item) {
    assert(item->get_state() == item_state::TO_MEM_DISK);

    flashcache::subdevice& subdev = this->get_subdevice();
    auto sem = make_shared<semaphore>({ 0 });
    auto& item_data = item->data();
    auto item_size = item->size();
    auto blocks_to_store = (item_size + (flashcache::block_size - 1)) / flashcache::block_size;
    assert(item_data.size() == item_size);
    assert(item->used_blocks_empty());
    assert(blocks_to_store >= 1);

    auto to_write = item_size;
    item->used_blocks_resize(blocks_to_store);
    for (auto i = 0U; i < blocks_to_store; ++i) {
        auto write_size = std::min(to_write, flashcache::block_size);

        subdev.wait().then([&subdev, sem, item, write_size, i] {
            if (item->get_state() == item_state::ERASED) {
                return make_ready_future<>();
            }
            auto wbuf = allocate_aligned_buffer<unsigned char>(flashcache::block_size, flashcache::block_size);
            const char *data = item->data().c_str();
            assert(data != nullptr);
            assert((i * flashcache::block_size + write_size) <= item->data().size()); // overflow check
            memcpy(wbuf.get(), data + (i * flashcache::block_size), write_size);
            auto wb = wbuf.get();
            flashcache::block blk = subdev.allocate();
            item->use_block(i, blk);

            return subdev.write(blk, wb).then([] (size_t ret) mutable {
                assert(ret == flashcache::block_size);
            }).or_terminate();
        }).finally([&subdev, sem] {
            subdev.signal();
            sem->signal(1);
        });
        to_write -= write_size;
    }

    return sem->wait(blocks_to_store).then([this, item] () mutable {
        // NOTE: Item was removed previously from mem lru so as to avoid races, i.e.
        // upon another set, the same item would be popped from the back of the lru.
        auto& item_data = item->data();
        auto item_data_size = item_data.size();
        assert(item_data_size == item->size());

        if (item->get_state() != item_state::ERASED) {
            // Adjusting LRU: item is moved from mem lru to mem_disk lru.
            auto& item_ref = *item;
            item->set_state(item_state::MEM_DISK);
            _mem_disk_lru.push_front(item_ref);
        }
        this->_stats._stores++;
    });
}

template <bool WithFlashCache>
class cache : public std::conditional<WithFlashCache, flashcache_cache_base, memcache_cache_base>::type {
private:
    using item_type = item<WithFlashCache>;
    using cache_type = bi::unordered_set<item_type,
        bi::member_hook<item_type, typename item_type::hook_type, &item_type::_cache_link>,
        bi::power_2_buckets<true>,
        bi::constant_time_size<true>>;
    using cache_iterator = typename cache_type::iterator;
    using cache_bucket = typename cache_type::bucket_type;
    static constexpr size_t initial_bucket_count = 1 << 10;
    static constexpr float load_factor = 0.75f;
    size_t _resize_up_threshold = load_factor * initial_bucket_count;
    cache_bucket* _buckets;
    cache_type _cache;
    timer_set<item_type, &item_type::_timer_link> _alive;
    timer<> _timer;
    timer<> _flush_timer;
    memory::reclaimer _reclaimer;
private:
    size_t item_footprint(item_type& item_ref) {
        return sizeof(item_type) + item_ref._data.size() + item_ref.key().size();
    }

    template <bool IsInCache = true, bool IsInTimerList = true>
    void erase(item_type& item_ref) {
        if (IsInCache) {
            _cache.erase(_cache.iterator_to(item_ref));
        }
        if (IsInTimerList) {
            _alive.remove(item_ref);
        }
        this->do_erase(item_ref);
        this->_stats._bytes -= item_footprint(item_ref);
        intrusive_ptr_release(&item_ref);
    }

    void expire() {
        auto exp = _alive.expire(clock_type::now());
        while (!exp.empty()) {
            auto item = &*exp.begin();
            exp.pop_front();
            erase<true, false>(*item);
            this->_stats._expired++;
        }
        _timer.arm(_alive.get_next_timeout());
    }

    inline
    cache_iterator find(const item_key& key) {
        return _cache.find(key, std::hash<item_key>(), item_key_cmp<WithFlashCache>());
    }

    template <typename Origin>
    inline
    cache_iterator add_overriding(cache_iterator i, item_insertion_data& insertion) {
        auto& old_item = *i;

        auto new_item = new item_type(Origin::move_if_local(insertion.key), Origin::move_if_local(insertion.ascii_prefix),
            Origin::move_if_local(insertion.data), insertion.expiry, old_item._version + 1);
        intrusive_ptr_add_ref(new_item);

        erase(old_item);

        auto insert_result = _cache.insert(*new_item);
        assert(insert_result.second);
        if (_alive.insert(*new_item)) {
            _timer.rearm(new_item->get_timeout());
        }
        this->_lru.push_front(*new_item);
        this->do_set(*new_item);
        this->_stats._bytes += item_footprint(*new_item);
        return insert_result.first;
    }

    template <typename Origin>
    inline
    void add_new(item_insertion_data& insertion) {
        auto new_item = new item_type(Origin::move_if_local(insertion.key), Origin::move_if_local(insertion.ascii_prefix),
            Origin::move_if_local(insertion.data), insertion.expiry);
        intrusive_ptr_add_ref(new_item);
        auto& item_ref = *new_item;
        _cache.insert(item_ref);
        if (_alive.insert(item_ref)) {
            _timer.rearm(item_ref.get_timeout());
        }
        this->_lru.push_front(*new_item);
        this->do_set(*new_item);
        this->_stats._bytes += item_footprint(item_ref);
        maybe_rehash();
    }

    void maybe_rehash() {
        if (_cache.size() >= _resize_up_threshold) {
            auto new_size = _cache.bucket_count() * 2;
            auto old_buckets = _buckets;
            try {
                _buckets = new cache_bucket[new_size];
            } catch (const std::bad_alloc& e) {
                this->_stats._resize_failure++;
                evict(100); // In order to amortize the cost of resize failure
                return;
            }
            _cache.rehash(typename cache_type::bucket_traits(_buckets, new_size));
            delete[] old_buckets;
            _resize_up_threshold = _cache.bucket_count() * load_factor;
        }
    }

    // Evicts at most @count items.
    void evict(size_t count) {
        while (!this->_lru.empty() && count--) {
            erase(this->_lru.back());
            this->_stats._evicted++;
        }
    }

    void reclaim(size_t target) {
        size_t reclaimed_so_far = 0;
        this->_stats._reclaims++;

        reclaimed_so_far += this->do_reclaim(target);
        if (reclaimed_so_far >= target) {
            return;
        }

        auto i = this->_lru.end();
        if (i == this->_lru.begin()) {
            return;
        }

        --i;

        bool done = false;
        do {
            item_type& victim = *i;
            if (i != this->_lru.begin()) {
                --i;
            } else {
                done = true;
            }

            // If the item is shared, we can not assume that removing it from
            // cache would cause the memory to be reclaimed in a timely manner
            // so we reclaim only items which are not shared.
            if (victim._ref_count == 1) {
                reclaimed_so_far += item_footprint(victim);
                erase(victim);
                this->_stats._evicted++;

                if (reclaimed_so_far >= target) {
                    done = true;
                }
            }
        } while (!done);
    }
public:
    cache()
        : _buckets(new cache_bucket[initial_bucket_count])
        , _cache(typename cache_type::bucket_traits(_buckets, initial_bucket_count))
        , _reclaimer([this] { reclaim(5*MB); })
    {
        _timer.set_callback([this] { expire(); });
        _flush_timer.set_callback([this] { flush_all(); });
    }

    future<> setup(foreign_ptr<shared_ptr<flashcache::devfile>> dev, uint64_t offset, uint64_t length) {
        this->do_setup(std::move(dev), offset, length);
        return make_ready_future<>();
    }

    void flush_all() {
        _flush_timer.cancel();
        _cache.erase_and_dispose(_cache.begin(), _cache.end(), [this] (item_type* it) {
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
            this->_stats._set_replaces++;
            return true;
        } else {
            add_new<Origin>(insertion);
            this->_stats._set_adds++;
            return false;
        }
    }

    bool remote_set(item_insertion_data& insertion) {
        return set<remote_origin_tag>(insertion);
    }

    template <typename Origin = local_origin_tag>
    bool add(item_insertion_data& insertion) {
        auto i = find(insertion.key);
        if (i != _cache.end()) {
            return false;
        }

        this->_stats._set_adds++;
        add_new<Origin>(insertion);
        return true;
    }

    bool remote_add(item_insertion_data& insertion) {
        return add<remote_origin_tag>(insertion);
    }

    template <typename Origin = local_origin_tag>
    bool replace(item_insertion_data& insertion) {
        auto i = find(insertion.key);
        if (i == _cache.end()) {
            return false;
        }

        this->_stats._set_replaces++;
        add_overriding<Origin>(i, insertion);
        return true;
    }

    bool remote_replace(item_insertion_data& insertion) {
        return replace<remote_origin_tag>(insertion);
    }

    bool remove(const item_key& key) {
        auto i = find(key);
        if (i == _cache.end()) {
            this->_stats._delete_misses++;
            return false;
        }
        this->_stats._delete_hits++;
        auto& item_ref = *i;
        erase(item_ref);
        return true;
    }

    future<item_ptr<WithFlashCache>> get(const item_key& key) {
        auto i = find(key);
        if (i == _cache.end()) {
            this->_stats._get_misses++;
            return make_ready_future<item_ptr<WithFlashCache>>(nullptr);
        }
        this->_stats._get_hits++;
        auto& item_ref = *i;
        auto item = boost::intrusive_ptr<item_type>(&item_ref);
        return this->do_get(item).then([item] {
            return make_ready_future<item_ptr<WithFlashCache>>(make_foreign(item));
        });
    }

    template <typename Origin = local_origin_tag>
    cas_result cas(item_insertion_data& insertion, typename item_type::version_type version) {
        auto i = find(insertion.key);
        if (i == _cache.end()) {
            this->_stats._cas_misses++;
            return cas_result::not_found;
        }
        auto& item_ref = *i;
        if (item_ref._version != version) {
            this->_stats._cas_badval++;
            return cas_result::bad_version;
        }
        this->_stats._cas_hits++;
        add_overriding<Origin>(i, insertion);
        return cas_result::stored;
    }

    cas_result remote_cas(item_insertion_data& insertion, typename item_type::version_type version) {
        return cas<remote_origin_tag>(insertion, version);
    }

    size_t size() {
        return _cache.size();
    }

    size_t bucket_count() {
        return _cache.bucket_count();
    }

    cache_stats stats() {
        this->_stats._size = size();
        return this->_stats;
    }

    template <typename Origin = local_origin_tag>
    std::pair<item_ptr<WithFlashCache>, bool> incr(item_key& key, uint64_t delta) {
        auto i = find(key);
        if (i == _cache.end()) {
            this->_stats._incr_misses++;
            return {item_ptr<WithFlashCache>{}, false};
        }
        auto& item_ref = *i;
        this->_stats._incr_hits++;
        auto value = item_ref.data_as_integral();
        if (!value) {
            return {boost::intrusive_ptr<item_type>(&item_ref), false};
        }
        item_insertion_data insertion {
            .key = Origin::move_if_local(key),
            .ascii_prefix = item_ref._ascii_prefix,
            .data = to_sstring(*value + delta),
            .expiry = item_ref._expiry
        };
        i = add_overriding<local_origin_tag>(i, insertion);
        return {boost::intrusive_ptr<item_type>(&*i), true};
    }

    std::pair<item_ptr<WithFlashCache>, bool> remote_incr(item_key& key, uint64_t delta) {
        return incr<remote_origin_tag>(key, delta);
    }

    template <typename Origin = local_origin_tag>
    std::pair<item_ptr<WithFlashCache>, bool> decr(item_key& key, uint64_t delta) {
        auto i = find(key);
        if (i == _cache.end()) {
            this->_stats._decr_misses++;
            return {item_ptr<WithFlashCache>{}, false};
        }
        auto& item_ref = *i;
        this->_stats._decr_hits++;
        auto value = item_ref.data_as_integral();
        if (!value) {
            return {boost::intrusive_ptr<item_type>(&item_ref), false};
        }
        item_insertion_data insertion {
            .key = Origin::move_if_local(key),
            .ascii_prefix = item_ref._ascii_prefix,
            .data = to_sstring(*value - std::min(*value, delta)),
            .expiry = item_ref._expiry
        };
        i = add_overriding<local_origin_tag>(i, insertion);
        return {boost::intrusive_ptr<item_type>(&*i), true};
    }

    std::pair<item_ptr<WithFlashCache>, bool> remote_decr(item_key& key, uint64_t delta) {
        return decr<remote_origin_tag>(key, delta);
    }

    std::pair<unsigned, foreign_ptr<shared_ptr<std::string>>> print_hash_stats() {
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
        return {engine.cpu_id(), make_foreign(make_shared(ss.str()))};
    }

    future<> stop() { return make_ready_future<>(); }
};

template <bool WithFlashCache>
class sharded_cache {
private:
    distributed<cache<WithFlashCache>>& _peers;

    inline
    unsigned get_cpu(const item_key& key) {
        return std::hash<item_key>()(key) % smp::count;
    }
public:
    sharded_cache(distributed<cache<WithFlashCache>>& peers) : _peers(peers) {}

    future<> flush_all() {
        return _peers.invoke_on_all(&cache<WithFlashCache>::flush_all);
    }

    future<> flush_at(clock_type::time_point time_point) {
        return _peers.invoke_on_all(&cache<WithFlashCache>::flush_at, time_point);
    }

    // The caller must keep @insertion live until the resulting future resolves.
    future<bool> set(item_insertion_data& insertion) {
        auto cpu = get_cpu(insertion.key);
        if (engine.cpu_id() == cpu) {
            return make_ready_future<bool>(_peers.local().set(insertion));
        }
        return _peers.invoke_on(cpu, &cache<WithFlashCache>::remote_set, std::ref(insertion));
    }

    // The caller must keep @insertion live until the resulting future resolves.
    future<bool> add(item_insertion_data& insertion) {
        auto cpu = get_cpu(insertion.key);
        if (engine.cpu_id() == cpu) {
            return make_ready_future<bool>(_peers.local().add(insertion));
        }
        return _peers.invoke_on(cpu, &cache<WithFlashCache>::remote_add, std::ref(insertion));
    }

    // The caller must keep @insertion live until the resulting future resolves.
    future<bool> replace(item_insertion_data& insertion) {
        auto cpu = get_cpu(insertion.key);
        if (engine.cpu_id() == cpu) {
            return make_ready_future<bool>(_peers.local().replace(insertion));
        }
        return _peers.invoke_on(cpu, &cache<WithFlashCache>::remote_replace, std::ref(insertion));
    }

    // The caller must keep @key live until the resulting future resolves.
    future<bool> remove(const item_key& key) {
        auto cpu = get_cpu(key);
        return _peers.invoke_on(cpu, &cache<WithFlashCache>::remove, std::ref(key));
    }

    // The caller must keep @key live until the resulting future resolves.
    future<item_ptr<WithFlashCache>> get(const item_key& key) {
        auto cpu = get_cpu(key);
        return _peers.invoke_on(cpu, &cache<WithFlashCache>::get, std::ref(key));
    }

    // The caller must keep @insertion live until the resulting future resolves.
    future<cas_result> cas(item_insertion_data& insertion, typename item<WithFlashCache>::version_type version) {
        auto cpu = get_cpu(insertion.key);
        if (engine.cpu_id() == cpu) {
            return make_ready_future<cas_result>(_peers.local().cas(insertion, version));
        }
        return _peers.invoke_on(cpu, &cache<WithFlashCache>::remote_cas, std::ref(insertion), std::move(version));
    }

    future<cache_stats> stats() {
        return _peers.map_reduce(adder<cache_stats>(), &cache<WithFlashCache>::stats);
    }

    // The caller must keep @key live until the resulting future resolves.
    future<std::pair<item_ptr<WithFlashCache>, bool>> incr(item_key& key, uint64_t delta) {
        auto cpu = get_cpu(key);
        if (engine.cpu_id() == cpu) {
            return make_ready_future<std::pair<item_ptr<WithFlashCache>, bool>>(
                _peers.local().incr(key, delta));
        }
        return _peers.invoke_on(cpu, &cache<WithFlashCache>::remote_incr, std::ref(key), std::move(delta));
    }

    // The caller must keep @key live until the resulting future resolves.
    future<std::pair<item_ptr<WithFlashCache>, bool>> decr(item_key& key, uint64_t delta) {
        auto cpu = get_cpu(key);
        if (engine.cpu_id() == cpu) {
            return make_ready_future<std::pair<item_ptr<WithFlashCache>, bool>>(
                _peers.local().decr(key, delta));
        }
        return _peers.invoke_on(cpu, &cache<WithFlashCache>::remote_decr, std::ref(key), std::move(delta));
    }

    future<> print_hash_stats(output_stream<char>& out) {
        return _peers.map_reduce([&out] (std::pair<unsigned, foreign_ptr<shared_ptr<std::string>>> data) mutable {
            return out.write("=== CPU " + std::to_string(data.first) + " ===\r\n")
                .then([&out, str = std::move(data.second)] {
                    return out.write(*str);
                });
            }, &cache<WithFlashCache>::print_hash_stats);
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

template <bool WithFlashCache>
class ascii_protocol {
private:
    using this_type = ascii_protocol<WithFlashCache>;
    sharded_cache<WithFlashCache>& _cache;
    distributed<system_stats>& _system_stats;
    memcache_ascii_parser _parser;
    item_key _item_key;
    item_insertion_data _insertion;
    std::vector<item_ptr<WithFlashCache>> _items;
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
    template <bool WithVersion>
    static void append_item(scattered_message<char>& msg, item_ptr<WithFlashCache> item) {
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
        msg.append_static(item->data());
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
                    return this->print_stat(out, "pid", getpid())
                        .then([this, now, &out, uptime = now - all_system_stats._start_time] {
                            return this->print_stat(out, "uptime",
                                std::chrono::duration_cast<std::chrono::seconds>(uptime).count());
                        }).then([this, now, &out] {
                            return this->print_stat(out, "time",
                                std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
                        }).then([this, &out] {
                            return this->print_stat(out, "version", VERSION_STRING);
                        }).then([this, &out] {
                            return this->print_stat(out, "pointer_size", sizeof(void*)*8);
                        }).then([this, &out, v = all_system_stats._curr_connections] {
                            return this->print_stat(out, "curr_connections", v);
                        }).then([this, &out, v = all_system_stats._total_connections] {
                            return this->print_stat(out, "total_connections", v);
                        }).then([this, &out, v = all_system_stats._curr_connections] {
                            return this->print_stat(out, "connection_structures", v);
                        }).then([this, &out, v = all_system_stats._cmd_get] {
                            return this->print_stat(out, "cmd_get", v);
                        }).then([this, &out, v = all_system_stats._cmd_set] {
                            return this->print_stat(out, "cmd_set", v);
                        }).then([this, &out, v = all_system_stats._cmd_flush] {
                            return this->print_stat(out, "cmd_flush", v);
                        }).then([this, &out] {
                            return this->print_stat(out, "cmd_touch", 0);
                        }).then([this, &out, v = all_cache_stats._get_hits] {
                            return this->print_stat(out, "get_hits", v);
                        }).then([this, &out, v = all_cache_stats._get_misses] {
                            return this->print_stat(out, "get_misses", v);
                        }).then([this, &out, v = all_cache_stats._delete_misses] {
                            return this->print_stat(out, "delete_misses", v);
                        }).then([this, &out, v = all_cache_stats._delete_hits] {
                            return this->print_stat(out, "delete_hits", v);
                        }).then([this, &out, v = all_cache_stats._incr_misses] {
                            return this->print_stat(out, "incr_misses", v);
                        }).then([this, &out, v = all_cache_stats._incr_hits] {
                            return this->print_stat(out, "incr_hits", v);
                        }).then([this, &out, v = all_cache_stats._decr_misses] {
                            return this->print_stat(out, "decr_misses", v);
                        }).then([this, &out, v = all_cache_stats._decr_hits] {
                            return this->print_stat(out, "decr_hits", v);
                        }).then([this, &out, v = all_cache_stats._cas_misses] {
                            return this->print_stat(out, "cas_misses", v);
                        }).then([this, &out, v = all_cache_stats._cas_hits] {
                            return this->print_stat(out, "cas_hits", v);
                        }).then([this, &out, v = all_cache_stats._cas_badval] {
                            return this->print_stat(out, "cas_badval", v);
                        }).then([this, &out] {
                            return this->print_stat(out, "touch_hits", 0);
                        }).then([this, &out] {
                            return this->print_stat(out, "touch_misses", 0);
                        }).then([this, &out] {
                            return this->print_stat(out, "auth_cmds", 0);
                        }).then([this, &out] {
                            return this->print_stat(out, "auth_errors", 0);
                        }).then([this, &out] {
                            return this->print_stat(out, "threads", smp::count);
                        }).then([this, &out, v = all_cache_stats._size] {
                            return this->print_stat(out, "curr_items", v);
                        }).then([this, &out, v = total_items] {
                            return this->print_stat(out, "total_items", v);
                        }).then([this, &out, v = all_cache_stats._expired] {
                            return this->print_stat(out, "seastar.expired", v);
                        }).then([this, &out, v = all_cache_stats._resize_failure] {
                            return this->print_stat(out, "seastar.resize_failure", v);
                        }).then([this, &out, v = all_cache_stats._evicted] {
                            return this->print_stat(out, "evicted", v);
                        }).then([this, &out, v = all_cache_stats._bytes] {
                            return this->print_stat(out, "bytes", v);
                        }).then([&out] {
                            return out.write(msg_end);
                        });
                });
        });
    }
public:
    ascii_protocol(sharded_cache<WithFlashCache>& cache, distributed<system_stats>& system_stats)
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

    void prepare_insertion() {
        _insertion = item_insertion_data{
            .key = std::move(_parser._key),
            .ascii_prefix = make_sstring(" ", _parser._flags_str, " ", _parser._size_str),
            .data = std::move(_parser._blob),
            .expiry = seconds_to_time_point(_parser._expiration)
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
                        auto f = _cache.flush_at(seconds_to_time_point(_parser._expiration));
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
                        return out.write(item->data()).then([&out] {
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
                        return out.write(item->data()).then([&out] {
                            return out.write(msg_crlf);
                        });
                    });
                }
            };
            std::abort();
        });
    };
};

template <bool WithFlashCache>
class udp_server {
public:
    static const size_t default_max_datagram_size = 1400;
private:
    sharded_cache<WithFlashCache>& _cache;
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
        ascii_protocol<WithFlashCache> _proto;

        connection(ipv4_addr src, uint16_t request_id, input_stream<char>&& in, size_t out_size,
                sharded_cache<WithFlashCache>& c, distributed<system_stats>& system_stats)
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
    udp_server(sharded_cache<WithFlashCache>& c, distributed<system_stats>& system_stats, uint16_t port = 11211)
         : _cache(c)
         , _system_stats(system_stats)
         , _port(port)
    {}

    void set_max_datagram_size(size_t max_datagram_size) {
        _max_datagram_size = max_datagram_size;
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

                header hdr = ntoh(*p.get_header<header>());
                p.trim_front(sizeof(hdr));

                auto request_id = hdr._request_id;
                auto in = as_input_stream(std::move(p));
                auto conn = make_shared<connection>(dgram.get_src(), request_id, std::move(in),
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

template <bool WithFlashCache>
class tcp_server {
private:
    shared_ptr<server_socket> _listener;
    sharded_cache<WithFlashCache>& _cache;
    distributed<system_stats>& _system_stats;
    uint16_t _port;
    struct connection {
        connected_socket _socket;
        socket_address _addr;
        input_stream<char> _in;
        output_stream<char> _out;
        ascii_protocol<WithFlashCache> _proto;
        distributed<system_stats>& _system_stats;
        connection(connected_socket&& socket, socket_address addr, sharded_cache<WithFlashCache>& c, distributed<system_stats>& system_stats)
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
    tcp_server(sharded_cache<WithFlashCache>& cache, distributed<system_stats>& system_stats, uint16_t port = 11211)
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

    future<> stop() { return make_ready_future<>(); }
};

template <bool WithFlashCache>
class stats_printer {
private:
    timer<> _timer;
    sharded_cache<WithFlashCache>& _cache;
public:
    stats_printer(sharded_cache<WithFlashCache>& cache)
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
                if (WithFlashCache) {
                    std::cout << " reclaims: " << stats._reclaims << " "
                        << "loads: " << stats._loads << " "
                        << "stores: " << stats._stores << " ";
                }
                std::cout << std::endl;
            });
        });
        _timer.arm_periodic(std::chrono::seconds(1));
    }

    future<> stop() { return make_ready_future<>(); }
};

} /* namespace memcache */

template <bool WithFlashCache>
int start_instance(int ac, char** av) {
    distributed<memcache::cache<WithFlashCache>> cache_peers;
    memcache::sharded_cache<WithFlashCache> cache(cache_peers);
    distributed<memcache::system_stats> system_stats;
    distributed<memcache::udp_server<WithFlashCache>> udp_server;
    distributed<memcache::tcp_server<WithFlashCache>> tcp_server;
    memcache::stats_printer<WithFlashCache> stats(cache);

    namespace bpo = boost::program_options;
    app_template app;
    if (WithFlashCache) {
        app.add_options()
            ("device", bpo::value<std::string>(),
                "Flash device")
            ;
    }
    app.add_options()
        ("max-datagram-size", bpo::value<int>()->default_value(memcache::udp_server<WithFlashCache>::default_max_datagram_size),
             "Maximum size of UDP datagram")
        ("stats",
             "Print basic statistics periodically (every second)")
        ;

    return app.run(ac, av, [&] {
        engine.at_exit([&] { return tcp_server.stop(); });
        engine.at_exit([&] { return udp_server.stop(); });
        engine.at_exit([&] { return cache_peers.stop(); });
        engine.at_exit([&] { return system_stats.stop(); });

        auto&& config = app.configuration();
        return cache_peers.start().then([&system_stats] {
            return system_stats.start(clock_type::now());
        }).then([&] {
            if (WithFlashCache) {
                auto device_path = config["device"].as<std::string>();
                return engine.open_file_dma(device_path).then([&] (file f) {
                    auto dev = make_shared<flashcache::devfile>({std::move(f)});
                    return dev->f().stat().then([&, dev] (struct stat st) mutable {
                        assert(S_ISBLK(st.st_mode));
                        return dev->f().size().then([&, dev] (size_t device_size) mutable {
                            auto per_cpu_device_size = device_size / smp::count;
                            std::cout << PLATFORM << " flashcached " << VERSION << "\n";
                            std::cout << "device size: " << device_size << " bytes\n";
                            std::cout << "per-cpu device size: " << per_cpu_device_size << " bytes\n";

                            for (auto cpu = 0U; cpu < smp::count; cpu++) {
                                auto offset = cpu * per_cpu_device_size;
                                cache_peers.invoke_on(cpu, &memcache::cache<WithFlashCache>::setup,
                                    make_foreign(dev), std::move(offset), std::move(per_cpu_device_size));
                            }
                        });
                    });
                });
            } else {
                std::cout << PLATFORM << " memcached " << VERSION << "\n";
                return make_ready_future<>();
            }
        }).then([&] {
            return tcp_server.start(std::ref(cache), std::ref(system_stats));
        }).then([&tcp_server] {
            return tcp_server.invoke_on_all(&memcache::tcp_server<WithFlashCache>::start);
        }).then([&] {
            if (engine.net().has_per_core_namespace()) {
                return udp_server.start(std::ref(cache), std::ref(system_stats));
            } else {
                return udp_server.start_single(std::ref(cache), std::ref(system_stats));
            }
        }).then([&] {
            return udp_server.invoke_on_all(&memcache::udp_server<WithFlashCache>::set_max_datagram_size,
                    (size_t)config["max-datagram-size"].as<int>());
        }).then([&] {
            return udp_server.invoke_on_all(&memcache::udp_server<WithFlashCache>::start);
        }).then([&stats, start_stats = config.count("stats")] {
            if (start_stats) {
                stats.start();
            }
        });
    });
}

int memcache_instance<false>::run(int ac, char** av) {
    return start_instance<false>(ac, av);
}

int memcache_instance<true>::run(int ac, char** av) {
    return start_instance<true>(ac, av);
}
