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
#include <stack>
#include "core/scollectd.hh"
#include "core/align.hh"

namespace bi = boost::intrusive;

/*
 * Item requirements
 * - Extend it to slab_item_base.
 * - First parameter of constructor must be uint8_t _slab_class_id.
 * - Implement get_slab_class_id() to return _slab_class_id.
 * - Implement is_unlocked() to check if Item can be evicted.
 */

class slab_item_base {
    bi::list_member_hook<> _lru_link;

    template<typename Item>
    friend class slab_class;
};

template<typename Item>
class slab_class {
private:
    std::vector<void *> _slab_pages;
    std::stack<void *> _free_objects;
    bi::list<slab_item_base, bi::member_hook<slab_item_base, bi::list_member_hook<>, &slab_item_base::_lru_link>> _lru;
    size_t _size; // size of objects
    uint8_t _slab_class_id;
private:
    template<typename... Args>
    inline Item* create_item(void *object, Args&&... args) {
        Item *new_item = new(object) Item(_slab_class_id, std::forward<Args>(args)...);
        _lru.push_front(reinterpret_cast<slab_item_base&>(*new_item));
        return new_item;
    }

    inline void* evict_lru_item(std::function<void (Item& item_ref)>& erase_func) {
        if (_lru.empty()) {
            return nullptr;
        }

        Item& victim = reinterpret_cast<Item&>(_lru.back());
        assert(victim.is_unlocked());
        _lru.erase(_lru.iterator_to(reinterpret_cast<slab_item_base&>(victim)));
        // WARNING: You need to make sure that erase_func will not release victim back to slab.
        erase_func(victim);

        return reinterpret_cast<void*>(&victim);
    }
public:
    slab_class(size_t size, uint8_t slab_class_id)
        : _size(size)
        , _slab_class_id(slab_class_id)
    {
    }
    slab_class(slab_class&&) = default;
    ~slab_class() {
        _lru.clear();
        for (auto& slab : _slab_pages) {
            free(slab);
        }
    }

    size_t size() const {
        return _size;
    }

    bool empty() const {
        return _free_objects.empty();
    }

    bool has_no_slab_pages() const {
        return _slab_pages.empty();
    }

    template<typename... Args>
    Item *create(Args&&... args) {
        assert(!_free_objects.empty());
        auto object = _free_objects.top();
        _free_objects.pop();

        return create_item(object, std::forward<Args>(args)...);
    }

    template<typename... Args>
    Item *create_from_new_page(uint64_t max_object_size, Args&&... args) {
        constexpr size_t alignment = std::alignment_of<Item>::value;
        void *slab_page = aligned_alloc(alignment, max_object_size);
        if (!slab_page) {
            throw std::bad_alloc{};
        }
        _slab_pages.push_back(slab_page);

        assert(_size % alignment == 0);
        auto objects = max_object_size / _size;
        auto object = reinterpret_cast<uint8_t *>(slab_page);
        for (auto i = 1u; i < objects; i++) {
            object += _size;
            _free_objects.push(object);
        }

        // first object from the allocated slab page is returned.
        return create_item(slab_page, std::forward<Args>(args)...);
    }

    template<typename... Args>
    Item *create_from_lru(std::function<void (Item& item_ref)>& erase_func, Args&&... args) {
        auto victim_object = evict_lru_item(erase_func);
        if (!victim_object) {
            throw std::bad_alloc{};
        }
        return create_item(victim_object, std::forward<Args>(args)...);
    }

    void free_item(Item *item) {
        void *object = item;
        _lru.erase(_lru.iterator_to(reinterpret_cast<slab_item_base&>(*item)));
        _free_objects.push(object);
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
};

template<typename Item>
class slab_allocator {
private:
    std::vector<size_t> _slab_class_sizes;
    std::vector<slab_class<Item>> _slab_classes;
    std::vector<scollectd::registration> _registrations;
    std::function<void (Item& item_ref)> _erase_func;
    uint64_t _max_object_size;
    uint64_t _available_slab_pages;
    struct collectd_stats {
        uint64_t allocs;
        uint64_t frees;
    } _stats;
private:
    void initialize_slab_classes(double growth_factor, uint64_t limit) {
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

    slab_class<Item>* get_slab_class(Item* item) {
        auto slab_class_id = item->get_slab_class_id();
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
public:
    slab_allocator(double growth_factor, uint64_t limit, uint64_t max_object_size)
        : _max_object_size(max_object_size)
        , _available_slab_pages(limit / max_object_size)
    {
        initialize_slab_classes(growth_factor, limit);
        register_collectd_metrics();
    }

    slab_allocator(double growth_factor, uint64_t limit, uint64_t max_object_size, std::function<void (Item& item_ref)> erase_func)
        : _erase_func(std::move(erase_func))
        , _max_object_size(max_object_size)
        , _available_slab_pages(limit / max_object_size)
    {
        initialize_slab_classes(growth_factor, limit);
        register_collectd_metrics();
    }

    ~slab_allocator()
    {
        _registrations.clear();
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
            if (_available_slab_pages > 0 || slab_class->has_no_slab_pages()) {
                item = slab_class->create_from_new_page(_max_object_size, std::forward<Args>(args)...);
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
        // remove item from the lru of its slab class.
        auto slab_class = get_slab_class(item);
        slab_class->remove_item_from_lru(item);
    }

    void unlock_item(Item *item) {
        // insert item into the lru of its slab class.
        auto slab_class = get_slab_class(item);
        slab_class->insert_item_into_lru(item);
    }

    /**
     * Free an item back to its original slab class.
     */
    void free(Item *item) {
        if (item) {
            auto slab_class = get_slab_class(item);
            slab_class->free_item(item);
            _stats.frees++;
        }
    }

    /**
     * Update item position in the LRU of its slab class.
     */
    void touch(Item *item) {
        if (item) {
            auto slab_class = get_slab_class(item);
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
