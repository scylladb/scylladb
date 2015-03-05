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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 * To compile: g++ -std=c++14 slab_test.cc
 */

#include <iostream>
#include <assert.h>
#include "core/slab.hh"

static constexpr size_t max_object_size = 1024*1024;

class item : public slab_item_base {
public:
    bi::list_member_hook<> _cache_link;
    uint32_t _slab_page_index;

    item(uint32_t slab_page_index) : _slab_page_index(slab_page_index) {}

    const uint32_t get_slab_page_index() {
        return _slab_page_index;
    }
    const bool is_unlocked() {
        return true;
    }
};

template<typename Item>
static void free_vector(slab_allocator<Item>& slab, std::vector<item *>& items) {
    for (auto item : items) {
        slab.free(item);
    }
}

static void test_allocation_1(const double growth_factor, const unsigned slab_limit_size) {
    slab_allocator<item> slab(growth_factor, slab_limit_size, max_object_size);
    size_t size = max_object_size;

    slab.print_slab_classes();

    std::vector<item *> items;

    assert(slab_limit_size % size == 0);
    for (auto i = 0u; i < (slab_limit_size / size); i++) {
        auto item = slab.create(size);
        items.push_back(item);
    }
    assert(slab.create(size) == nullptr);

    free_vector<item>(slab, items);
    std::cout << __FUNCTION__ << " done!\n";
}

static void test_allocation_2(const double growth_factor, const unsigned slab_limit_size) {
    slab_allocator<item> slab(growth_factor, slab_limit_size, max_object_size);
    size_t size = 1024;

    std::vector<item *> items;

    auto allocations = 0u;
    for (;;) {
        auto item = slab.create(size);
        if (!item) {
            break;
        }
        items.push_back(item);
        allocations++;
    }

    auto class_size = slab.class_size(size);
    auto per_slab_page = max_object_size / class_size;
    auto available_slab_pages = slab_limit_size / max_object_size;
    assert(allocations == (per_slab_page * available_slab_pages));

    free_vector<item>(slab, items);
    std::cout << __FUNCTION__ << " done!\n";
}

static void test_allocation_with_lru(const double growth_factor, const unsigned slab_limit_size) {
    bi::list<item, bi::member_hook<item, bi::list_member_hook<>, &item::_cache_link>> _cache;
    unsigned evictions = 0;

    slab_allocator<item> slab(growth_factor, slab_limit_size, max_object_size,
        [&](item& item_ref) { _cache.erase(_cache.iterator_to(item_ref)); evictions++; });
    size_t size = max_object_size;

    auto max = slab_limit_size / max_object_size;
    for (auto i = 0u; i < max * 1000; i++) {
        auto item = slab.create(size);
        assert(item != nullptr);
        _cache.push_front(*item);
    }
    assert(evictions == max * 999);

    _cache.clear();

    std::cout << __FUNCTION__ << " done!\n";
}

int main(int ac, char** av) {
    test_allocation_1(1.25, 5*1024*1024);
    test_allocation_2(1.07, 5*1024*1024); // 1.07 is the growth factor used by facebook.
    test_allocation_with_lru(1.25, 5*1024*1024);

    return 0;
}
