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
 * Copyright 2015 Cloudius Systems
 */
#ifndef __SLAB_ALLOCATOR__
#define __SLAB_ALLOCATOR__

#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list.hpp>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <memory>
#include <vector>
#include <algorithm>
#include "core/scollectd.hh"
#include "core/align.hh"
#include "core/memory.hh"

static constexpr uint16_t SLAB_MAGIC_NUMBER = 0x51AB; // meant to be 'SLAB' :-)
typedef uint64_t uintptr_t;

namespace bi = boost::intrusive;

/*
 * Item requirements
 * - Extend it to slab_item_base.
 * - First parameter of constructor must be uint32_t _slab_page_index.
 * - Implement get_slab_page_index() to return _slab_page_index.
 * - Implement is_unlocked() to check if Item can be evicted.
 */

/*
 * slab_page_desc is 1:1 mapped to slab page.
 * footprint: 80b for each slab page.
 */
struct slab_page_desc {
private:
    bi::list_member_hook<> _lru_link;
    bi::list_member_hook<> _free_pages_link;
    void *_slab_page;
    std::vector<uintptr_t> _free_objects;
    uint32_t _refcnt;
    uint32_t _index; // index into slab page vector
    uint16_t _magic;
    uint8_t _slab_class_id;
public:
    slab_page_desc(void *slab_page, size_t objects, size_t object_size, uint8_t slab_class_id, uint32_t index)
        : _slab_page(slab_page)
        , _refcnt(0U)
        , _index(index)
        , _magic(SLAB_MAGIC_NUMBER)
        , _slab_class_id(slab_class_id)
    {
        auto object = reinterpret_cast<uintptr_t>(slab_page);
        _free_objects.reserve(objects - 1);
        for (auto i = 1u; i < objects; i++) {
            object += object_size;
            _free_objects.push_back(object);
        }
    }

    bool empty() const {
        return _free_objects.empty();
    }

    size_t size() const {
        return _free_objects.size();
    }

    uint32_t& refcnt() {
        return _refcnt;
    }

    uint32_t index() const {
        return _index;
    }

    uint16_t magic() const {
        return _magic;
    }

    uint8_t slab_class_id() const {
        return _slab_class_id;
    }

    void* slab_page() const {
        return _slab_page;
    }

    std::vector<uintptr_t>& free_objects() {
        return _free_objects;
    }

    void* allocate_object() {
        assert(!_free_objects.empty());
        auto object = reinterpret_cast<void*>(_free_objects.back());
        _free_objects.pop_back();
        return object;
    }

    void free_object(void *object) {
        _free_objects.push_back(reinterpret_cast<uintptr_t>(object));
    }

    template<typename Item>
    friend class slab_class;
    template<typename Item>
    friend class slab_allocator;
};

class slab_item_base {
    bi::list_member_hook<> _lru_link;

    template<typename Item>
    friend class slab_class;
};

template<typename Item>
class slab_class {
private:
    bi::list<slab_page_desc,
        bi::member_hook<slab_page_desc, bi::list_member_hook<>,
        &slab_page_desc::_free_pages_link>> _free_slab_pages;
    bi::list<slab_item_base,
        bi::member_hook<slab_item_base, bi::list_member_hook<>,
        &slab_item_base::_lru_link>> _lru;
    size_t _size; // size of objects
    uint8_t _slab_class_id;
private:
    template<typename... Args>
    inline
    Item* create_item(void *object, uint32_t slab_page_index, Args&&... args) {
        Item *new_item = new(object) Item(slab_page_index, std::forward<Args>(args)...);
        _lru.push_front(reinterpret_cast<slab_item_base&>(*new_item));
        return new_item;
    }

    inline
    std::pair<void *, uint32_t> evict_lru_item(std::function<void (Item& item_ref)>& erase_func) {
        if (_lru.empty()) {
            return { nullptr, 0U };
        }

        Item& victim = reinterpret_cast<Item&>(_lru.back());
        uint32_t index = victim.get_slab_page_index();
        assert(victim.is_unlocked());
        _lru.erase(_lru.iterator_to(reinterpret_cast<slab_item_base&>(victim)));
        // WARNING: You need to make sure that erase_func will not release victim back to slab.
        erase_func(victim);

        return { reinterpret_cast<void*>(&victim), index };
    }
public:
    slab_class(size_t size, uint8_t slab_class_id)
        : _size(size)
        , _slab_class_id(slab_class_id)
    {
    }
    slab_class(slab_class&&) = default;
    ~slab_class() {
        _free_slab_pages.clear();
        _lru.clear();
    }

    size_t size() const {
        return _size;
    }

    bool empty() const {
        return _free_slab_pages.empty();
    }

    bool has_no_slab_pages() const {
        return _lru.empty();
    }

    template<typename... Args>
    Item *create(Args&&... args) {
        assert(!_free_slab_pages.empty());
        auto& desc = _free_slab_pages.back();
        auto object = desc.allocate_object();
        if (desc.empty()) {
            // if empty, remove desc from the list of slab pages with free objects.
            _free_slab_pages.erase(_free_slab_pages.iterator_to(desc));
        }

        return create_item(object, desc.index(), std::forward<Args>(args)...);
    }

    template<typename... Args>
    Item *create_from_new_page(uint64_t max_object_size, uint32_t slab_page_index,
                               std::function<void (slab_page_desc& desc)> insert_slab_page_desc,
                               Args&&... args) {
        // allocate slab page.
        constexpr size_t alignment = std::alignment_of<Item>::value;
        void *slab_page = aligned_alloc(alignment, max_object_size);
        if (!slab_page) {
            throw std::bad_alloc{};
        }
        // allocate descriptor to slab page.
        slab_page_desc *desc = nullptr;
        assert(_size % alignment == 0);
        try {
            auto objects = max_object_size / _size;
            desc = new slab_page_desc(slab_page, objects, _size, _slab_class_id, slab_page_index);
        } catch (const std::bad_alloc& e) {
            // FIXME: Is there really a need to re-throw std::bad_alloc?
            throw std::bad_alloc{};
        }

        _free_slab_pages.push_front(*desc);
        insert_slab_page_desc(*desc);

        // first object from the allocated slab page is returned.
        return create_item(slab_page, slab_page_index, std::forward<Args>(args)...);
    }

    template<typename... Args>
    Item *create_from_lru(std::function<void (Item& item_ref)>& erase_func, Args&&... args) {
        auto ret = evict_lru_item(erase_func);
        if (!ret.first) {
            throw std::bad_alloc{};
        }
        return create_item(ret.first, ret.second, std::forward<Args>(args)...);
    }

    void free_item(Item *item, slab_page_desc& desc) {
        void *object = item;
        _lru.erase(_lru.iterator_to(reinterpret_cast<slab_item_base&>(*item)));
        desc.free_object(object);
        if (desc.size() == 1) {
            // push back desc into the list of slab pages with free objects.
            _free_slab_pages.push_back(desc);
        }
    }

    void touch_item(Item *item) {
        auto& item_ref = reinterpret_cast<slab_item_base&>(*item);
        _lru.erase(_lru.iterator_to(item_ref));
        _lru.push_front(item_ref);
    }

    void remove_item_from_lru(Item *item) {
        auto& item_ref = reinterpret_cast<slab_item_base&>(*item);
        _lru.erase(_lru.iterator_to(item_ref));
    }

    void insert_item_into_lru(Item *item) {
        auto& item_ref = reinterpret_cast<slab_item_base&>(*item);
        _lru.push_front(item_ref);
    }

    void remove_desc_from_free_list(slab_page_desc& desc) {
        assert(desc.slab_class_id() == _slab_class_id);
        _free_slab_pages.erase(_free_slab_pages.iterator_to(desc));
    }
};

template<typename Item>
class slab_allocator {
private:
    std::vector<size_t> _slab_class_sizes;
    std::vector<slab_class<Item>> _slab_classes;
    std::vector<scollectd::registration> _registrations;
    // erase_func() is used to remove the item from the cache using slab.
    std::function<void (Item& item_ref)> _erase_func;
    std::vector<slab_page_desc*> _slab_pages_vector;
    bi::list<slab_page_desc,
        bi::member_hook<slab_page_desc, bi::list_member_hook<>,
        &slab_page_desc::_lru_link>> _slab_page_desc_lru;
    uint64_t _max_object_size;
    uint64_t _available_slab_pages;
    struct collectd_stats {
        uint64_t allocs;
        uint64_t frees;
    } _stats;
    memory::reclaimer *_reclaimer = nullptr;
    bool _reclaimed = false;
private:
    void evict_lru_slab_page() {
        if (_slab_page_desc_lru.empty()) {
            // NOTE: Nothing to evict. If this happens, it implies that all
            // slab pages in the slab are being used at the same time.
            // That being said, this event is very unlikely to happen.
            return;
        }
        // get descriptor of the least-recently-used slab page and related info.
        auto& desc = _slab_page_desc_lru.back();
        assert(desc.refcnt() == 0);
        uint8_t slab_class_id = desc.slab_class_id();
        auto slab_class = get_slab_class(slab_class_id);
        void *slab_page = desc.slab_page();

        auto& free_objects = desc.free_objects();
        if (!desc.empty()) {
            // if not empty, remove desc from the list of slab pages with free objects.
            slab_class->remove_desc_from_free_list(desc);
            // and sort the array of free objects for binary search later on.
            std::sort(free_objects.begin(), free_objects.end());
        }
        // remove desc from the list of slab page descriptors.
        _slab_page_desc_lru.erase(_slab_page_desc_lru.iterator_to(desc));
        // remove desc from the slab page vector.
        _slab_pages_vector[desc.index()] = nullptr;

        // Iterate through objects in the slab page and if the object is an allocated
        // item, the item should be removed from LRU and then erased.
        uintptr_t object = reinterpret_cast<uintptr_t>(slab_page);
        auto object_size = slab_class->size();
        auto objects = _max_object_size / object_size;
        for (auto i = 0u; i < objects; i++, object += object_size) {
            if (!desc.empty()) {
                // if binary_search returns false, it means that object at the current
                // offset isn't an item.
                if (std::binary_search(free_objects.begin(), free_objects.end(), object)) {
                    continue;
                }
            }
            Item* item = reinterpret_cast<Item*>(object);
            assert(item->is_unlocked());
            slab_class->remove_item_from_lru(item);
            _erase_func(*item);
            _stats.frees++;
        }
#ifdef DEBUG
        printf("lru slab page eviction succeeded! desc_empty?=%d\n", desc.empty());
#endif
        ::free(slab_page); // free slab page object
        delete &desc; // free its descriptor
    }

    /*
     * Reclaim the least recently used slab page that is unused.
     */
    void reclaim() {
        // once reclaimer was called, slab pages should no longer be allocated, as the
        // memory used by slab is supposed to be calibrated.
        _reclaimed = true;
        // FIXME: Should reclaim() only evict a single slab page at a time?
        evict_lru_slab_page();
    }

    void initialize_slab_allocator(double growth_factor, uint64_t limit) {
        constexpr size_t alignment = std::alignment_of<Item>::value;
        constexpr size_t initial_size = 96;
        size_t size = initial_size; // initial object size
        uint8_t slab_class_id = 0U;

        while (_max_object_size / size > 1) {
            size = align_up(size, alignment);
            _slab_class_sizes.push_back(size);
            _slab_classes.emplace_back(size, slab_class_id);
            size *= growth_factor;
            assert(slab_class_id < std::numeric_limits<uint8_t>::max());
            slab_class_id++;
        }
        _slab_class_sizes.push_back(_max_object_size);
        _slab_classes.emplace_back(_max_object_size, slab_class_id);

        // If slab limit is zero, enable reclaimer.
        if (!limit) {
            _reclaimer = new memory::reclaimer([this] { reclaim(); });
        } else {
            _slab_pages_vector.reserve(_available_slab_pages);
        }
    }

    slab_class<Item>* get_slab_class(const size_t size) {
        // given a size, find slab class with binary search.
        auto i = std::lower_bound(_slab_class_sizes.begin(), _slab_class_sizes.end(), size);
        if (i == _slab_class_sizes.end()) {
            return nullptr;
        }
        auto dist = std::distance(_slab_class_sizes.begin(), i);
        return &_slab_classes[dist];
    }

    slab_class<Item>* get_slab_class(const uint8_t slab_class_id) {
        assert(slab_class_id >= 0 && slab_class_id < _slab_classes.size());
        return &_slab_classes[slab_class_id];
    }

    void register_collectd_metrics() {
        auto add = [this] (auto type_name, auto name, auto data_type, auto func) {
            _registrations.push_back(
                scollectd::add_polled_metric(scollectd::type_instance_id("slab",
                    scollectd::per_cpu_plugin_instance,
                    type_name, name),
                    scollectd::make_typed(data_type, func)));
        };

        add("total_operations", "malloc", scollectd::data_type::DERIVE, [&] { return _stats.allocs; });
        add("total_operations", "free", scollectd::data_type::DERIVE, [&] { return _stats.frees; });
        add("objects", "malloc", scollectd::data_type::GAUGE, [&] { return _stats.allocs - _stats.frees; });
    }

    inline slab_page_desc& get_slab_page_desc(Item *item)
    {
        auto desc = _slab_pages_vector[item->get_slab_page_index()];
        assert(desc != nullptr);
        assert(desc->magic() == SLAB_MAGIC_NUMBER);
        return *desc;
    }

    inline bool can_allocate_page(slab_class<Item>& sc) {
        return (_reclaimer && !_reclaimed) ||
            (_available_slab_pages > 0 || sc.has_no_slab_pages());
    }
public:
    slab_allocator(double growth_factor, uint64_t limit, uint64_t max_object_size)
        : _max_object_size(max_object_size)
        , _available_slab_pages(limit / max_object_size)
    {
        initialize_slab_allocator(growth_factor, limit);
        register_collectd_metrics();
    }

    slab_allocator(double growth_factor, uint64_t limit, uint64_t max_object_size,
                   std::function<void (Item& item_ref)> erase_func)
        : _erase_func(std::move(erase_func))
        , _max_object_size(max_object_size)
        , _available_slab_pages(limit / max_object_size)
    {
        initialize_slab_allocator(growth_factor, limit);
        register_collectd_metrics();
    }

    ~slab_allocator()
    {
        _slab_page_desc_lru.clear();
        for (auto desc : _slab_pages_vector) {
            if (!desc) {
                continue;
            }
            ::free(desc->slab_page());
            delete desc;
        }
        _registrations.clear();
        delete _reclaimer;
    }

    /**
     * Create an item from a given slab class based on requested size.
     */
    template<typename... Args>
    Item* create(const size_t size, Args&&... args) {
        auto slab_class = get_slab_class(size);
        if (!slab_class) {
            throw std::bad_alloc{};
        }

        Item *item = nullptr;
        if (!slab_class->empty()) {
            item = slab_class->create(std::forward<Args>(args)...);
            _stats.allocs++;
        } else {
            if (can_allocate_page(*slab_class)) {
                auto index_to_insert = _slab_pages_vector.size();
                item = slab_class->create_from_new_page(_max_object_size, index_to_insert,
                    [this](slab_page_desc& desc) {
                        if (_reclaimer) {
                            // insert desc into the LRU list of slab page descriptors.
                            _slab_page_desc_lru.push_front(desc);
                        }
                        // insert desc into the slab page vector.
                        _slab_pages_vector.push_back(&desc);
                    },
                    std::forward<Args>(args)...);
                if (_available_slab_pages > 0) {
                    _available_slab_pages--;
                }
                _stats.allocs++;
            } else if (_erase_func) {
                item = slab_class->create_from_lru(_erase_func, std::forward<Args>(args)...);
            }
        }
        return item;
    }

    void lock_item(Item *item) {
        auto& desc = get_slab_page_desc(item);
        if (_reclaimer) {
            auto& refcnt = desc.refcnt();

            if (++refcnt == 1) {
                // remove slab page descriptor from list of slab page descriptors.
                _slab_page_desc_lru.erase(_slab_page_desc_lru.iterator_to(desc));
            }
        }
        // remove item from the lru of its slab class.
        auto slab_class = get_slab_class(desc.slab_class_id());
        slab_class->remove_item_from_lru(item);
    }

    void unlock_item(Item *item) {
        auto& desc = get_slab_page_desc(item);
        if (_reclaimer) {
            auto& refcnt = desc.refcnt();

            if (--refcnt == 0) {
                // insert slab page descriptor back into list of slab page descriptors.
                _slab_page_desc_lru.push_front(desc);
            }
        }
        // insert item into the lru of its slab class.
        auto slab_class = get_slab_class(desc.slab_class_id());
        slab_class->insert_item_into_lru(item);
    }

    /**
     * Free an item back to its original slab class.
     */
    void free(Item *item) {
        if (item) {
            auto& desc = get_slab_page_desc(item);
            auto slab_class = get_slab_class(desc.slab_class_id());
            slab_class->free_item(item, desc);
            _stats.frees++;
        }
    }

    /**
     * Update item position in the LRU of its slab class.
     */
    void touch(Item *item) {
        if (item) {
            auto& desc = get_slab_page_desc(item);
            auto slab_class = get_slab_class(desc.slab_class_id());
            slab_class->touch_item(item);
        }
    }

    /**
     * Helper function: Print all available slab classes and their respective properties.
     */
    void print_slab_classes() {
        auto class_id = 0;
        for (auto& slab_class : _slab_classes) {
            size_t size = slab_class.size();
            printf("slab[%3d]\tsize: %10lu\tper-slab-page: %5lu\n", class_id, size, _max_object_size / size);
            class_id++;
        }
    }

    /**
     * Helper function: Useful for getting a slab class' chunk size from a size parameter.
     */
    size_t class_size(const size_t size) {
        auto slab_class = get_slab_class(size);
        return (slab_class) ? slab_class->size() : 0;
    }
};

#endif /* __SLAB_ALLOCATOR__ */
