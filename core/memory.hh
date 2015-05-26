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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef MEMORY_HH_
#define MEMORY_HH_

#include "resource.hh"
#include <new>
#include <functional>
#include <vector>

namespace memory {

// TODO: Use getpagesize() in order to learn a size of a system PAGE.
static constexpr size_t page_bits = 12;
static constexpr size_t page_size = 1 << page_bits;       // 4K
static constexpr size_t huge_page_size = 512 * page_size; // 2M

void configure(std::vector<resource::memory> m,
        std::experimental::optional<std::string> hugetlbfs_path = {});

void* allocate_reclaimable(size_t size);

class reclaimer {
    std::function<void ()> _reclaim;
public:
    reclaimer(std::function<void ()> reclaim);
    ~reclaimer();
    void do_reclaim() { _reclaim(); }
};

// Call periodically to recycle objects that were freed
// on cpu other than the one they were allocated on.
//
// Returns @true if any work was actually performed.
bool drain_cross_cpu_freelist();

// We don't want the memory code calling back into the rest of
// the system, so allow the rest of the system to tell the memory
// code how to initiate reclaim.
//
// When memory is low, calling hook(fn) will result in fn being called
// in a safe place wrt. allocations.
void set_reclaim_hook(
        std::function<void (std::function<void ()>)> hook);

using physical_address = uint64_t;

struct translation {
    translation() = default;
    translation(physical_address a, size_t s) : addr(a), size(s) {}
    physical_address addr = 0;
    size_t size = 0;
};

// Translate a virtual address range to a physical range.
//
// Can return a smaller range (in which case the reminder needs
// to be translated again), or a zero sized range in case the
// translation is not known.
translation translate(const void* addr, size_t size);

class statistics;
statistics stats();

class statistics {
    uint64_t _mallocs;
    uint64_t _frees;
    uint64_t _cross_cpu_frees;
private:
    statistics(uint64_t mallocs, uint64_t frees, uint64_t cross_cpu_frees)
        : _mallocs(mallocs), _frees(frees), _cross_cpu_frees(cross_cpu_frees) {}
public:
    uint64_t mallocs() const { return _mallocs; }
    uint64_t frees() const { return _frees; }
    uint64_t cross_cpu_frees() const { return _cross_cpu_frees; }
    size_t live_objects() const { return mallocs() - frees(); }
    friend statistics stats();
};


}

class with_alignment {
    size_t _align;
public:
    with_alignment(size_t align) : _align(align) {}
    size_t alignment() const { return _align; }
};

void* operator new(size_t size, with_alignment wa);
void* operator new[](size_t size, with_alignment wa);
void operator delete(void* ptr, with_alignment wa);
void operator delete[](void* ptr, with_alignment wa);

template <typename T, size_t Align>
class aligned_allocator {
public:
    using value_type = T;
    T* allocate(size_t n) const {
        // FIXME: multiply can overflow?
        return reinterpret_cast<T*>(::operator new(n * sizeof(T), with_alignment(Align)));
    }
    void deallocate(T* ptr) const {
        return ::operator delete(ptr, with_alignment(Align));
    }
    bool operator==(const aligned_allocator& x) const {
        return true;
    }
    bool operator!=(const aligned_allocator& x) const {
        return false;
    }
};

#endif /* MEMORY_HH_ */
