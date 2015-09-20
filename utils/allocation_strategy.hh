/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <cstdlib>
#include <seastar/core/memory.hh>

template <typename T>
inline
void standard_migrator(void* src, void* dst, size_t) noexcept {
    static_assert(std::is_nothrow_move_constructible<T>::value, "T must be nothrow move-constructible.");
    static_assert(std::is_nothrow_destructible<T>::value, "T must be nothrow destructible.");

    T* src_t = static_cast<T*>(src);
    new (static_cast<T*>(dst)) T(std::move(*src_t));
    src_t->~T();
}

//
// Abstracts allocation strategy for managed objects.
//
// Managed objects may be moved by the allocator during compaction, which
// invalidates any references to those objects. Compaction may be started
// synchronously with allocations. To ensure that references remain valid, use
// logalloc::compaction_lock.
//
// Because references may get invalidated, managing allocators can't be used
// with standard containers, because they assume the reference is valid until freed.
//
// For example containers compatible with compacting allocators see:
//   - managed_ref - managed version of std::unique_ptr<>
//   - managed_bytes - managed version of "bytes"
//
// Note: When object is used as an element inside intrusive containers,
// typically no extra measures need to be taken for reference tracking, if the
// link member is movable. When object is moved, the member hook will be moved
// too and it should take care of updating any back-references. The user must
// be aware though that any iterators into such container may be invalidated
// across deferring points.
//
class allocation_strategy {
public:
    // A function used by compacting collectors to migrate objects during
    // compaction. The function should reconstruct the object located at src
    // in the location pointed by dst. The object at old location should be
    // destroyed. See standard_migrator() above for example. Both src and dst
    // are aligned as requested during alloc()/construct().
    using migrate_fn = void (*)(void* src, void* dst, size_t size) noexcept;

    virtual ~allocation_strategy() {}

    //
    // Allocates space for a new ManagedObject. The caller must construct the
    // object before compaction runs. "size" is the amount of space to reserve
    // in bytes. It can be larger than MangedObjects's size.
    //
    // Throws std::bad_alloc on allocation failure.
    //
    // Doesn't invalidate references to objects allocated with this strategy.
    //
    virtual void* alloc(migrate_fn, size_t size, size_t alignment) = 0;

    // Releases storage for the object. Doesn't invoke object's destructor.
    // Doesn't invalidate references to objects allocated with this strategy.
    virtual void free(void*) = 0;

    // Like alloc() but also constructs the object with a migrator using
    // standard move semantics. Allocates respecting object's alignment
    // requirement.
    template<typename T, typename... Args>
    T* construct(Args&&... args) {
        void* storage = alloc(standard_migrator<T>, sizeof(T), alignof(T));
        try {
            return new (storage) T(std::forward<Args>(args)...);
        } catch (...) {
            free(storage);
            throw;
        }
    }

    // Destroys T and releases its storage.
    // Doesn't invalidate references to allocated objects.
    template<typename T>
    void destroy(T* obj) {
        obj->~T();
        free(obj);
    }
};

class standard_allocation_strategy : public allocation_strategy {
public:
    virtual void* alloc(migrate_fn, size_t size, size_t alignment) override {
        // ASAN doesn't intercept aligned_alloc() and complains on free().
        void* ret;
        if (posix_memalign(&ret, alignment, size) != 0) {
            throw std::bad_alloc();
        }
        return ret;
    }

    virtual void free(void* obj) override {
        ::free(obj);
    }
};

extern standard_allocation_strategy standard_allocation_strategy_instance;

inline
standard_allocation_strategy& standard_allocator() {
    return standard_allocation_strategy_instance;
}

inline
allocation_strategy*& current_allocation_strategy_ptr() {
    static thread_local allocation_strategy* current = &standard_allocation_strategy_instance;
    return current;
}

inline
allocation_strategy& current_allocator() {
    return *current_allocation_strategy_ptr();
}

template<typename T>
inline
auto current_deleter() {
    auto& alloc = current_allocator();
    return [&alloc] (T* obj) {
        alloc.destroy(obj);
    };
}


//
// Passing allocators to objects.
//
// The same object type can be allocated using different allocators, for
// example standard allocator (for temporary data), or log-structured
// allocator for long-lived data. In case of LSA, objects may be allocated
// inside different LSA regions. Objects should be freed only from the region
// which owns it.
//
// There's a problem of how to ensure correct usage of allocators. Storing the
// reference to the allocator used for construction of some object inside that
// object is a possible solution. This has a disadvantage of extra space
// overhead per-object though. We could avoid that if the code which decides
// about which allocator to use is also the code which controls object's life
// time. That seems to be the case in current uses, so a simplified scheme of
// passing allocators will do. Allocation strategy is set in a thread-local
// context, as shown below. From there, aware objects pick up the allocation
// strategy. The code controling the objects must ensure that object allocated
// in one regime is also freed in the same regime.
//
// with_allocator() provides a way to set the current allocation strategy used
// within given block of code. with_allocator() can be nested, which will
// temporarily shadow enclosing strategy. Use current_allocator() to obtain
// currently active allocation strategy. Use current_deleter() to obtain a
// Deleter object using current allocation strategy to destroy objects.
//
// Example:
//
//   logalloc::region r;
//   with_allocator(r.allocator(), [] {
//       auto obj = make_managed<int>();
//   });
//

class allocator_lock {
    allocation_strategy* _prev;
public:
    allocator_lock(allocation_strategy& alloc) {
        _prev = current_allocation_strategy_ptr();
        current_allocation_strategy_ptr() = &alloc;
    }

    ~allocator_lock() {
        current_allocation_strategy_ptr() = _prev;
    }
};

template<typename Func>
inline
decltype(auto) with_allocator(allocation_strategy& alloc, Func&& func) {
    allocator_lock l(alloc);
    return func();
}
