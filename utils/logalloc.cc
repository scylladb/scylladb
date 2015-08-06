/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/heap/binomial_heap.hpp>

#include <seastar/core/memory.hh>
#include <seastar/core/align.hh>
#include <seastar/core/print.hh>

#include "utils/logalloc.hh"
#include "log.hh"

namespace logalloc {

struct segment;

static logging::logger logger("lsa");
static thread_local tracker tracker_instance;

using eviction_fn = std::function<void(size_t)>;

class tracker::impl {
    std::vector<region::impl*> _regions;
    scollectd::registrations _collectd_registrations;
private:
    void register_collectd_metrics();
public:
    impl() {
        register_collectd_metrics();
    }
    ~impl() {
        assert(_regions.empty());
    }
    void register_region(region::impl*);
    void unregister_region(region::impl*);
    uint64_t reclaim(uint64_t bytes);
    void full_compaction();
    occupancy_stats occupancy() const;
};

tracker::tracker()
    : _impl(std::make_unique<impl>())
    , _reclaimer([this] {
        reclaim(10*1024*1024);
    })
{ }

tracker::~tracker() {
}

uint64_t tracker::reclaim(uint64_t bytes) {
    return _impl->reclaim(bytes);
}

occupancy_stats tracker::occupancy() const {
    return _impl->occupancy();
}

void tracker::full_compaction() {
    return _impl->full_compaction();
}

tracker& shard_tracker() {
    return tracker_instance;
}

struct segment_occupancy_descending_less_compare {
    inline bool operator()(segment* s1, segment* s2) const;
};

// FIXME: The choice of data structure was arbitrary, evaluate different heap variants.
// Consider using an intrusive container leveraging segment_descriptor objects.
using segment_heap = boost::heap::binomial_heap<
    segment*, boost::heap::compare<segment_occupancy_descending_less_compare>>;

struct segment {
    using size_type = uint16_t;
    static constexpr int size_shift = 15; // 32K
    static constexpr size_type size = 1 << size_shift;

    uint8_t data[size];

    template<typename T>
    const T* at(size_t offset) const {
        return reinterpret_cast<const T*>(data + offset);
    }

    template<typename T>
    T* at(size_t offset) {
        return reinterpret_cast<T*>(data + offset);
    }

    bool is_empty() const;
    void record_alloc(size_type size);
    void record_free(size_type size);
    occupancy_stats occupancy() const;

    void set_heap_handle(segment_heap::handle_type);
    const segment_heap::handle_type& heap_handle();
} __attribute__((__aligned__(segment::size)));

inline bool
segment_occupancy_descending_less_compare::operator()(segment* s1, segment* s2) const {
    return s2->occupancy() < s1->occupancy();
}

struct segment_descriptor {
    bool _lsa_managed;
    segment::size_type _free_space;
    segment_heap::handle_type _heap_handle;

    segment_descriptor()
        : _lsa_managed(false)
    { }

    bool is_empty() const {
        return _free_space == segment::size;
    }
};

#ifndef DEFAULT_ALLOCATOR

// Segment pool implementation for the seastar allocator.
// Stores segment descriptors in a vector which is indexed using most significant
// bits of segment address.
class segment_pool {
    std::vector<segment_descriptor> _segments;
    uintptr_t _segments_base; // The address of the first segment
    uint64_t _segments_in_use{};
    memory::memory_layout _layout;
public:
    segment_pool();
    segment* new_segment();
    segment_descriptor& descriptor(const segment*);
    void free_segment(segment*);
    uint64_t segments_in_use() const;
    bool is_lsa_managed(segment*);
};

segment_descriptor&
segment_pool::descriptor(const segment* seg) {
    uintptr_t seg_addr = reinterpret_cast<uintptr_t>(seg);
    uintptr_t index = (seg_addr - _segments_base) >> segment::size_shift;
    return _segments[index];
}

segment*
segment_pool::new_segment() {
    auto seg = new (with_alignment(segment::size)) segment();

    // segment_pool::descriptor() is relying on segment alignment
    auto seg_addr = reinterpret_cast<uintptr_t>(seg);
    assert((seg_addr & ((uintptr_t)segment::size - 1)) == 0);

    ++_segments_in_use;
    segment_descriptor& desc = descriptor(seg);
    desc._lsa_managed = true;
    desc._free_space = segment::size;
    return seg;
}

void segment_pool::free_segment(segment* seg) {
    logger.debug("Releasing segment {}", seg);
    descriptor(seg)._lsa_managed = false;
    delete seg;
    --_segments_in_use;
}

segment_pool::segment_pool()
    : _layout(memory::get_memory_layout())
{
    // We're relying here on the fact that segments are aligned to their size.
    _segments_base = align_up(_layout.start, (uintptr_t) segment::size);
    _segments.resize((_layout.end - _segments_base) / segment::size);
}

#else

// Segment pool version for the standard allocator. Slightly less efficient
// than the version for seastar's allocator.
class segment_pool {
    std::unordered_map<const segment*, segment_descriptor> _segments;
    uint64_t _segments_in_use{};
public:
    segment* new_segment() {
        ++_segments_in_use;
        auto seg = new (with_alignment(segment::size)) segment();
        assert((reinterpret_cast<uintptr_t>(seg) & (sizeof(segment) - 1)) == 0);
        segment_descriptor& desc = _segments[seg];
        desc._lsa_managed = true;
        desc._free_space = segment::size;
        return seg;
    }
    segment_descriptor& descriptor(const segment* seg) {
        auto i = _segments.find(seg);
        if (i != _segments.end()) {
            return i->second;
        } else {
            segment_descriptor& desc = _segments[seg];
            desc._lsa_managed = false;
            return desc;
        }
    }
    void free_segment(segment* seg) {
        --_segments_in_use;
        auto i = _segments.find(seg);
        assert(i != _segments.end());
        _segments.erase(i);
        delete seg;
    }
    uint64_t segments_in_use() const;
    bool is_lsa_managed(segment*);
};

#endif

bool
segment_pool::is_lsa_managed(segment* seg) {
    return descriptor(seg)._lsa_managed;
}

uint64_t segment_pool::segments_in_use() const {
    return _segments_in_use;
}

static thread_local segment_pool shard_segment_pool;

void segment::record_alloc(segment::size_type size) {
    shard_segment_pool.descriptor(this)._free_space -= size;
}

void segment::record_free(segment::size_type size) {
    shard_segment_pool.descriptor(this)._free_space += size;
}

bool segment::is_empty() const {
    return shard_segment_pool.descriptor(this).is_empty();
}

occupancy_stats
segment::occupancy() const {
    return { shard_segment_pool.descriptor(this)._free_space, segment::size };
}

void
segment::set_heap_handle(segment_heap::handle_type handle) {
    shard_segment_pool.descriptor(this)._heap_handle = handle;
}

const segment_heap::handle_type&
segment::heap_handle() {
    return shard_segment_pool.descriptor(this)._heap_handle;
}

//
// For interface documentation see logalloc::region and allocation_strategy.
//
// Allocation dynamics.
//
// Objects are allocated inside fixed-size segments. Objects don't cross
// segment boundary. Active allocations are served from a single segment using
// bump-the-pointer method. That segment is called the active segment. When
// active segment fills up, it is closed. Closed segments are kept in a heap
// which orders them by occupancy. As objects are freed, the segment become
// sparser and are eventually released. Objects which are too large are
// allocated using standard allocator.
//
// Segment layout.
//
// Objects in a segment are laid out sequentially. Each object is preceded by
// a descriptor (see object_descriptor). Object alignment is respected, so if
// there is a gap between the end of current object and the next object's
// descriptor, a trunk of the object descriptor is left right after the
// current object with the flags byte indicating the amount of padding.
//
// Per-segment metadata is kept in a separate array, managed by segment_pool
// object.
//
class region::impl : public allocation_strategy {
    static constexpr float max_occupancy_for_compaction = 0.85; // FIXME: make configurable
    static constexpr size_t max_managed_object_size = segment::size * 0.1;

    // single-byte flags
    struct obj_flags {
        static constexpr uint8_t live_flag = 0x01;
        static constexpr uint8_t eos_flag = 0x02;
        static constexpr size_t max_alignment = (0xff >> 2) + 1;

        static uint8_t with_padding(uint8_t padding) {
            assert(padding < max_alignment);
            return uint8_t(padding << 2);
        }

        //
        // bit 0: 0 = dead, 1 = live
        // bit 1: when set, end-of-segment marker
        // bits 2-7: The value represents padding in bytes between the end of previous object
        //           and this object's descriptor. Must be smaller than object's alignment, so max alignment is 64.
        uint8_t _value;

        obj_flags(uint8_t value)
            : _value(value)
        { }

        static obj_flags make_end_of_segment() {
            return { eos_flag };
        }

        static obj_flags make_live(uint8_t padding) {
            return obj_flags(live_flag | with_padding(padding));
        }

        static obj_flags make_padding(uint8_t padding) {
            return obj_flags(with_padding(padding));
        }

        static obj_flags make_dead(uint8_t padding) {
            return obj_flags(with_padding(padding));
        }

        // Number of bytes preceding this descriptor after the end of the previous object
        uint8_t padding() const {
            return _value >> 2;
        }

        bool is_live() const {
            return _value & live_flag;
        }

        bool is_end_of_segment() const {
            return _value & eos_flag;
        }

        void mark_dead() {
            _value &= ~live_flag;
        }
    } __attribute__((packed));

    class object_descriptor {
    private:
        obj_flags _flags;
        uint8_t _alignment;
        segment::size_type _size;
        allocation_strategy::migrate_fn _migrator;
    public:
        object_descriptor(allocation_strategy::migrate_fn migrator, segment::size_type size, uint8_t alignment, uint8_t padding)
            : _flags(obj_flags::make_live(padding))
            , _alignment(alignment)
            , _size(size)
            , _migrator(migrator)
        { }

        void mark_dead() {
            _flags.mark_dead();
        }

        allocation_strategy::migrate_fn migrator() const {
            return _migrator;
        }

        uint8_t alignment() const {
            return _alignment;
        }

        segment::size_type size() const {
            return _size;
        }

        obj_flags flags() const {
            return _flags;
        }

        bool is_live() const {
            return _flags.is_live();
        }

        bool is_end_of_segment() const {
            return _flags.is_end_of_segment();
        }

        uint8_t padding() const {
            return _flags.padding();
        }

        friend std::ostream& operator<<(std::ostream& out, const object_descriptor& desc) {
            return out << sprint("{flags = %x, migrator=%p, alignment=%d, size=%d}",
                (int)desc._flags._value, desc._migrator, desc._alignment, desc._size);
        }
    } __attribute__((packed));
private:
    segment* _active = nullptr;
    size_t _active_offset;
    segment_heap _segments; // Contains only closed segments
    occupancy_stats _closed_occupancy;
    bool _compactible = true;
    bool _evictable = false;
    uint64_t _id;
    eviction_fn _eviction_fn;
private:
    void* alloc_small(allocation_strategy::migrate_fn migrator, segment::size_type size, size_t alignment) {
        assert(alignment < obj_flags::max_alignment);

        if (!_active) {
            _active = shard_segment_pool.new_segment();
            _active_offset = 0;
        }

        size_t obj_offset = align_up(_active_offset + sizeof(object_descriptor), alignment);
        if (obj_offset + size > segment::size) {
            close_and_open();
            return alloc_small(migrator, size, alignment);
        }

        auto descriptor_offset = obj_offset - sizeof(object_descriptor);
        auto padding = descriptor_offset - _active_offset;

        new (_active->data + _active_offset) obj_flags(obj_flags::make_padding(padding));
        new (_active->data + descriptor_offset) object_descriptor(migrator, size, alignment, padding);

        void* obj = _active->data + obj_offset;
        _active_offset = obj_offset + size;
        _active->record_alloc(size + sizeof(object_descriptor) + padding);
        return obj;
    }

    template<typename Func>
    void for_each_live(segment* seg, Func&& func) {
        static_assert(std::is_same<void, std::result_of_t<Func(object_descriptor*, void*)>>::value, "bad Func signature");

        size_t offset = 0;
        while (offset < segment::size) {
            object_descriptor* desc = seg->at<object_descriptor>(offset);
            offset += desc->flags().padding();
            desc = seg->at<object_descriptor>(offset);
            if (desc->is_end_of_segment()) {
                break;
            }
            offset += sizeof(object_descriptor);
            if (desc->is_live()) {
                func(desc, reinterpret_cast<void*>(seg->data + offset));
            }
            offset += desc->size();
        }
    }

    void close_active() {
        if (!_active) {
            return;
        }
        if (_active_offset < segment::size) {
            new (_active->data + _active_offset) obj_flags(obj_flags::make_end_of_segment());
        }
        logger.debug("Closing segment {}, used={}, waste={} [B]", _active, _active->occupancy(), segment::size - _active_offset);
        _closed_occupancy += _active->occupancy();
        auto handle = _segments.push(_active);
        _active->set_heap_handle(handle);
        _active = nullptr;
    }

    void compact(segment* seg) {
        for_each_live(seg, [this] (object_descriptor* desc, void* obj) {
            auto dst = alloc_small(desc->migrator(), desc->size(), desc->alignment());
            desc->migrator()(obj, dst, desc->size());
        });

        shard_segment_pool.free_segment(seg);
    }

    void close_and_open() {
        segment* new_active = shard_segment_pool.new_segment();
        close_active();
        _active = new_active;
        _active_offset = 0;
    }

    static uint64_t next_id() {
        static std::atomic<uint64_t> id{0};
        return id.fetch_add(1);
    }
public:
    impl()
        : _id(next_id())
    {
        tracker_instance._impl->register_region(this);
    }

    virtual ~impl() {
        tracker_instance._impl->unregister_region(this);

        assert(_segments.empty());
        if (_active) {
            assert(_active->is_empty());
            shard_segment_pool.free_segment(_active);
        }
    }

    impl(impl&&) = delete;
    impl(const impl&) = delete;

    occupancy_stats occupancy() const {
        occupancy_stats total{};
        total += _closed_occupancy;
        if (_active) {
            total += _active->occupancy();
        }
        return total;
    }

    occupancy_stats compactible_occupancy() const {
        return _closed_occupancy;
    }

    //
    // Returns true if this region can be compacted and compact() will make forward progress,
    // so that this will eventually stop:
    //
    //    while (is_compactible()) { compact(); }
    //
    bool is_compactible() const {
        return _compactible
            && (_closed_occupancy.free_space() >= 2 * segment::size)
            && (_closed_occupancy.used_fraction() < max_occupancy_for_compaction)
            && (_segments.top()->occupancy().free_space() >= max_managed_object_size);
    }

    virtual void* alloc(allocation_strategy::migrate_fn migrator, size_t size, size_t alignment) override {
        if (size > max_managed_object_size) {
            return standard_allocator().alloc(migrator, size, alignment);
        } else {
            return alloc_small(migrator, (segment::size_type) size, alignment);
        }
    }

    virtual void free(void* obj) override {
        auto obj_addr = reinterpret_cast<uintptr_t>(obj);
        auto segment_addr = align_down(obj_addr, (uintptr_t)segment::size);
        segment* seg = reinterpret_cast<segment*>(segment_addr);

        if (!shard_segment_pool.is_lsa_managed(seg)) {
            standard_allocator().free(obj);
            return;
        }

        auto desc = reinterpret_cast<object_descriptor*>(obj_addr - sizeof(object_descriptor));
        desc->mark_dead();

        if (seg != _active) {
            _closed_occupancy -= seg->occupancy();
        }

        seg->record_free(desc->size() + sizeof(object_descriptor) + desc->padding());

        if (seg != _active) {
            if (seg->is_empty()) {
                _segments.erase(seg->heap_handle());
                shard_segment_pool.free_segment(seg);
            } else {
                _closed_occupancy += seg->occupancy();
                _segments.decrease(seg->heap_handle());
            }
        }
    }

    // Merges another region into this region. The other region is left empty.
    // Doesn't invalidate references to allocated objects.
    void merge(region::impl& other) {
        if (!_active || _active->is_empty()) {
            _active = other._active;
            other._active = nullptr;
            _active_offset = other._active_offset;
        } else {
            other.close_active();
        }

        _segments.merge(other._segments);

        _closed_occupancy += other._closed_occupancy;
        other._closed_occupancy = {};
    }

    // Returns occupancy of the sparsest compactible segment.
    occupancy_stats min_occupancy() const {
        if (_segments.empty()) {
            return {};
        }
        return _segments.top()->occupancy();
    }

    // Tries to release one full segment back to the segment pool.
    void compact() {
        if (!is_compactible()) {
            return;
        }

        auto in_use = shard_segment_pool.segments_in_use();

        while (shard_segment_pool.segments_in_use() >= in_use) {
            segment* seg = _segments.top();
            logger.debug("Compacting segment {} from region {}, {}", seg, id(), seg->occupancy());
            _segments.pop();
            _closed_occupancy -= seg->occupancy();
            compact(seg);
        }
    }

    // Compacts everything. Mainly for testing.
    // Invalidates references to allocated objects.
    void full_compaction() {
        logger.debug("Full compaction, {}", occupancy());
        close_and_open();
        segment_heap all;
        std::swap(all, _segments);
        _closed_occupancy = {};
        while (!all.empty()) {
            segment* seg = all.top();
            all.pop();
            compact(seg);
        }
        logger.debug("Done, {}", occupancy());
    }

    allocation_strategy& allocator() {
        return *this;
    }

    uint64_t id() const {
        return _id;
    }

    void set_compactible(bool compactible) {
        _compactible = compactible;
    }

    void make_not_evictable() {
        _evictable = false;
        _eviction_fn = {};
    }

    void make_evictable(eviction_fn fn) {
        _evictable = true;
        _eviction_fn = std::move(fn);
    }
};

region::region()
    : _impl(std::make_unique<impl>())
{ }

region::~region() {
}

occupancy_stats region::occupancy() const {
    return _impl->occupancy();
}

void region::merge(region& other) {
    _impl->merge(*other._impl);
}

void region::full_compaction() {
    _impl->full_compaction();
}

allocation_strategy& region::allocator() {
    return *_impl;
}

void region::set_compactible(bool compactible) {
    _impl->set_compactible(compactible);
}

std::ostream& operator<<(std::ostream& out, const occupancy_stats& stats) {
    return out << sprint("%.2f%%, %d / %d [B]",
        stats.used_fraction() * 100, stats.used_space(), stats.total_space());
}

occupancy_stats tracker::impl::occupancy() const {
    occupancy_stats total{};
    for (auto&& r: _regions) {
        total += r->occupancy();
    }
    return total;
}

void tracker::impl::full_compaction() {
    logger.debug("Full compaction on all regions, {}", occupancy());

    for (region::impl* r : _regions) {
        if (r->is_compactible()) {
            r->full_compaction();
        }
    }

    logger.debug("Compaction done, {}", occupancy());
}

uint64_t tracker::impl::reclaim(uint64_t bytes) {
    //
    // Algorithm outline.
    //
    // Regions are kept in a max-heap ordered so that regions with
    // sparser segments are picked first. Non-compactible regions will be
    // picked last. In each iteration we try to release one whole segment from
    // the region which has the sparsest segment. We do it until we released
    // enough segments or there are no more regions we can compact.
    //
    // TODO: eviction step involving evictable pools.
    //

    auto segments_to_release = (bytes + segment::size - 1) >> segment::size_shift;

    auto cmp = [] (region::impl* c1, region::impl* c2) {
        if (c1->is_compactible() != c2->is_compactible()) {
            return !c1->is_compactible();
        }
        return c2->min_occupancy() < c1->min_occupancy();
    };

    uint64_t in_use = shard_segment_pool.segments_in_use();

    auto target = in_use - std::min(segments_to_release, in_use);

    logger.debug("Compacting, {} segments in use ({} B), trying to release {} ({} B).",
        in_use, in_use * segment::size, segments_to_release, segments_to_release * segment::size);

    boost::range::make_heap(_regions, cmp);

    if (logger.is_enabled(logging::log_level::debug)) {
        logger.debug("Occupancy of regions:");
        for (region::impl* r : _regions) {
            logger.debug(" - {}: min={}, avg={}", r->id(), r->min_occupancy(), r->compactible_occupancy());
        }
    }

    while (shard_segment_pool.segments_in_use() > target) {
        boost::range::pop_heap(_regions, cmp);
        region::impl* r = _regions.back();

        if (!r->is_compactible()) {
            logger.warn("Unable to release segments, no compactible pools.");
            // FIXME: Evict data from evictable pools.
            break;
        }

        r->compact();

        boost::range::push_heap(_regions, cmp);
    }

    uint64_t nr_released = in_use - shard_segment_pool.segments_in_use();
    logger.debug("Released {} segments.", nr_released);

    return nr_released * segment::size;
}

void tracker::impl::register_region(region::impl* r) {
    _regions.push_back(r);
    logger.debug("Registerred region @{} with id={}", r, r->id());
}

void tracker::impl::unregister_region(region::impl* r) {
    logger.debug("Unregisterring region, id={}", r->id());
    _regions.erase(std::remove(_regions.begin(), _regions.end(), r));
}

void tracker::impl::register_collectd_metrics() {
    _collectd_registrations = scollectd::registrations({
        scollectd::add_polled_metric(
            scollectd::type_instance_id("lsa", scollectd::per_cpu_plugin_instance, "bytes", "total_space"),
            scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return occupancy().total_space(); })
        ),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("lsa", scollectd::per_cpu_plugin_instance, "bytes", "used_space"),
            scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return occupancy().used_space(); })
        ),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("lsa", scollectd::per_cpu_plugin_instance, "bytes", "non_lsa_used_space"),
            scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return memory::stats().allocated_memory() - occupancy().used_space(); })
        ),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("lsa", scollectd::per_cpu_plugin_instance, "percent", "occupancy"),
            scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return occupancy().used_fraction() * 100; })
        ),
    });
}

}
