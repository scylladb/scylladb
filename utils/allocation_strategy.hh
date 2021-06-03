/*
 * Copyright (C) 2015-present ScyllaDB
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

#include <any>
#include <cstdlib>
#include <seastar/core/memory.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <malloc.h>

// A function used by compacting collectors to migrate objects during
// compaction. The function should reconstruct the object located at src
// in the location pointed by dst. The object at old location should be
// destroyed. See standard_migrator() above for example. Both src and dst
// are aligned as requested during alloc()/construct().
class migrate_fn_type {
    // Migrators may be registered by thread-local objects. The table of all
    // registered migrators is also thread-local which may cause problems with
    // the order of object destruction and lead to use-after-free.
    // This can be worked around by making migrators keep a shared pointer
    // to the table of migrators. std::any is used so that its type doesn't
    // have to be made public.
    std::any _migrators;
    uint32_t _align = 0;
    uint32_t _index;
private:
    static uint32_t register_migrator(migrate_fn_type* m);
    static void unregister_migrator(uint32_t index);
protected:
    explicit migrate_fn_type(size_t align) : _align(align), _index(register_migrator(this)) {}
public:
    virtual ~migrate_fn_type() { unregister_migrator(_index); }
    virtual void migrate(void* src, void* dsts, size_t size) const noexcept = 0;
    virtual size_t size(const void* obj) const = 0;
    size_t align() const { return _align; }
    uint32_t index() const { return _index; }
};

// Non-constant-size classes (ending with `char data[0]`) must provide
// the method telling the underlying storage size
template <typename T>
concept DynamicObject = requires (const T& obj) {
    { obj.storage_size() } noexcept -> std::same_as<size_t>;
};

template <typename T>
inline
size_t
size_for_allocation_strategy(const T& obj) noexcept {
    if constexpr (DynamicObject<T>) {
        return obj.storage_size();
    } else {
        return sizeof(T);
    }
}

template <typename T>
class standard_migrator final : public migrate_fn_type {
    friend class allocation_strategy;
    standard_migrator() : migrate_fn_type(alignof(T)) {}

public:
    virtual void migrate(void* src, void* dst, size_t size) const noexcept override {
        static_assert(std::is_nothrow_move_constructible<T>::value, "T must be nothrow move-constructible.");
        static_assert(std::is_nothrow_destructible<T>::value, "T must be nothrow destructible.");

        T* src_t = static_cast<T*>(src);
        new (static_cast<T*>(dst)) T(std::move(*src_t));
        src_t->~T();
    }
    virtual size_t size(const void* obj) const override {
        return size_for_allocation_strategy(*static_cast<const T*>(obj));
    }
};

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
    template <typename T>
    standard_migrator<T>& get_standard_migrator()
    {
        seastar::memory::scoped_critical_alloc_section dfg;
        static thread_local standard_migrator<T> instance;
        return instance;
    }

protected:
    size_t _preferred_max_contiguous_allocation = std::numeric_limits<size_t>::max();
    uint64_t _invalidate_counter = 1;
public:
    using migrate_fn = const migrate_fn_type*;

    virtual ~allocation_strategy() {}

    virtual void* alloc(migrate_fn, size_t size, size_t alignment) = 0;
    //
    // Allocates space for a new ManagedObject. The caller must construct the
    // object before compaction runs. "size" is the amount of space to reserve
    // in bytes. It can be larger than MangedObjects's size.
    //
    // Throws std::bad_alloc on allocation failure.
    //
    // Doesn't invalidate references to objects allocated with this strategy.
    //
    template <typename T>
    requires DynamicObject<T>
    void* alloc(size_t size) {
        return alloc(&get_standard_migrator<T>(), size, alignof(T));
    }

    // Releases storage for the object. Doesn't invoke object's destructor.
    // Doesn't invalidate references to objects allocated with this strategy.
    virtual void free(void* object, size_t size) = 0;
    virtual void free(void* object) = 0;

    // Returns the total immutable memory size used by the allocator to host
    // this object.  This will be at least the size of the object itself, plus
    // any immutable overhead needed to represent the object (if any).
    //
    // The immutable overhead is the overhead that cannot change over the
    // lifetime of the object (such as padding, etc).
    virtual size_t object_memory_size_in_allocator(const void* obj) const noexcept = 0;

    // Like alloc() but also constructs the object with a migrator using
    // standard move semantics. Allocates respecting object's alignment
    // requirement.
    template<typename T, typename... Args>
    T* construct(Args&&... args) {
        void* storage = alloc(&get_standard_migrator<T>(), sizeof(T), alignof(T));
        try {
            return new (storage) T(std::forward<Args>(args)...);
        } catch (...) {
            free(storage, sizeof(T));
            throw;
        }
    }

    // Destroys T and releases its storage.
    // Doesn't invalidate references to allocated objects.
    template<typename T>
    void destroy(T* obj) {
        size_t size = size_for_allocation_strategy(*obj);
        obj->~T();
        free(obj, size);
    }

    size_t preferred_max_contiguous_allocation() const noexcept {
        return _preferred_max_contiguous_allocation;
    }

    // Returns a number which is increased when references to objects managed by this allocator
    // are invalidated, e.g. due to internal events like compaction or eviction.
    // When the value returned by this method doesn't change, references obtained
    // between invocations remain valid.
    uint64_t invalidate_counter() const noexcept {
        return _invalidate_counter;
    }

    void invalidate_references() noexcept {
        ++_invalidate_counter;
    }
};

class standard_allocation_strategy : public allocation_strategy {
public:
    constexpr standard_allocation_strategy() {
        _preferred_max_contiguous_allocation = 128 * 1024;
    }
    virtual void* alloc(migrate_fn, size_t size, size_t alignment) override {
        seastar::memory::on_alloc_point();
        // ASAN doesn't intercept aligned_alloc() and complains on free().
        void* ret;
        // The system posix_memalign will return EINVAL if alignment is not
        // a multiple of pointer size.
        if (alignment < sizeof(void*)) {
            alignment = sizeof(void*);
        }
        if (posix_memalign(&ret, alignment, size) != 0) {
            throw std::bad_alloc();
        }
        return ret;
    }

    virtual void free(void* obj, size_t size) override {
        ::free(obj);
    }

    virtual void free(void* obj) override {
        ::free(obj);
    }

    virtual size_t object_memory_size_in_allocator(const void* obj) const noexcept {
        return ::malloc_usable_size(const_cast<void *>(obj));
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
    return [&alloc] (T* obj) noexcept {
        alloc.destroy(obj);
    };
}

template<typename T>
struct alloc_strategy_deleter {
    void operator()(T* ptr) const noexcept {
        current_allocator().destroy(ptr);
    }
};

// std::unique_ptr which can be used for owning an object allocated using allocation_strategy.
// Must be destroyed before the pointer is invalidated. For compacting allocators, that
// means it must not escape outside allocating_section or reclaim lock.
// Must be destroyed in the same allocating context in which T was allocated.
template<typename T>
using alloc_strategy_unique_ptr = std::unique_ptr<T, alloc_strategy_deleter<T>>;

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
