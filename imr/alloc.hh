/*
 * Copyright (C) 2018 ScyllaDB
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

#include "utils/chunked_vector.hh"
#include "utils/logalloc.hh"

#include "imr/core.hh"
#include "imr/methods.hh"

namespace imr {
namespace alloc {

static const struct no_context_factory_t {
    static no_context_t create(const void*) noexcept { return no_context; }
} no_context_factory;

/// Deserialisation context factory
///
/// Deserialisation contexts provide the IMR code with additional information
/// needed to deserialise an IMR object. Often the sources of that information
/// are both the object itself as well as some external state shared by multiple
/// IMR objects of the same type.
/// `context_factory` is a helper class for creating contexts it keeps the
/// shared state (e.g. per-schema information) and when given a pointer to a
/// IMR object creates a deserialisation context for it.
template<typename Context, typename... State>
class context_factory {
    std::tuple<State...> _state;
private:
    template<size_t... Index>
    Context create(const uint8_t* ptr, std::index_sequence<Index...>) const noexcept {
        return Context(ptr, std::get<Index>(_state)...);
    }
public:
    template<typename... Args>
    context_factory(Args&&... args) : _state(std::forward<Args>(args)...) { }

    context_factory(context_factory&) = default;
    context_factory(const context_factory&) = default;
    context_factory(context_factory&&) = default;

    Context create(const uint8_t* ptr) const noexcept {
        return create(ptr, std::index_sequence_for<State...>());
    }
};

GCC6_CONCEPT(
template<typename T>
concept bool ContextFactory = requires(const T factory, const uint8_t* ptr) {
    { factory.create(ptr) } noexcept;
};

static_assert(ContextFactory<no_context_factory_t>,
              "no_context_factory_t has to meet ContextFactory constraints");
)

/// LSA migrator for IMR objects
///
/// IMR objects may own memory and therefore moving and destroying them may
/// be non-trivial. This class implements an LSA migrator for an IMR objects
/// of type `Structure`. The deserialisation context needed to invoke the mover
/// is going to be created by the provided context factory `CtxFactory`.
template<typename Structure, typename CtxFactory>
GCC6_CONCEPT(requires ContextFactory<CtxFactory>)
class lsa_migrate_fn final : public migrate_fn_type, CtxFactory {
public:
    using structure = Structure;

    explicit lsa_migrate_fn(CtxFactory context_factory)
        : migrate_fn_type(1)
        , CtxFactory(std::move(context_factory))
    { }

    lsa_migrate_fn(lsa_migrate_fn&&) = delete;
    lsa_migrate_fn(const lsa_migrate_fn&) = delete;

    lsa_migrate_fn& operator=(lsa_migrate_fn&&) = delete;
    lsa_migrate_fn& operator=(const lsa_migrate_fn&) = delete;

    virtual void migrate(void* src_ptr, void* dst_ptr, size_t size) const noexcept override {
        std::memcpy(dst_ptr, src_ptr, size);
        auto dst = static_cast<uint8_t*>(dst_ptr);
        methods::move<Structure>(dst, CtxFactory::create(dst));
    }

    virtual size_t size(const void* obj_ptr) const noexcept override {
        auto ptr = static_cast<const uint8_t*>(obj_ptr);
        return Structure::serialized_object_size(ptr, CtxFactory::create(ptr));
    }
};

// LSA migrator for objects which mover doesn't require a deserialisation context
template<typename Structure>
struct default_lsa_migrate_fn {
    static lsa_migrate_fn<Structure, no_context_factory_t> migrate_fn;
};

template<typename Structure>
lsa_migrate_fn<Structure, no_context_factory_t> default_lsa_migrate_fn<Structure>::migrate_fn(no_context_factory);

/// IMR object allocator
///
/// This is a helper class that helps creating IMR objects that may own memory.
/// The serialisation of IMR objects is done in two phases:
/// 1. IMR figures out the size of the object. `sizer` provided by `get_sizer()`
///    records the size of all necessary memory allocations.
/// `allocate_all()` is called and allocates memory for all owned objects.
/// 2. Data is written to the allocated memory. `serializer` returned by
///    `get_serializer()` provides pointers to the allocated buffers and handles
///    their serialisation.
class object_allocator {
    union allocation {
        static_assert(std::is_trivially_destructible_v<std::pair<size_t, void*>>);
        static_assert(std::is_trivially_destructible_v<std::pair<size_t, allocation_strategy::migrate_fn>>);
    private:
        std::pair<size_t, allocation_strategy::migrate_fn> _allocation_request;
        std::pair<size_t, void*> _allocated_object;
    public:
        explicit allocation(size_t n, allocation_strategy::migrate_fn fn) noexcept
            : _allocation_request(std::make_pair(n, fn)) { }

        void allocate(allocation_strategy& allocator) {
            auto ptr = allocator.alloc(_allocation_request.second, _allocation_request.first, 1);
            _allocated_object = std::make_pair(_allocation_request.first, ptr);
        }

        void free(allocation_strategy& allocator) noexcept {
            allocator.free(_allocated_object.second, _allocated_object.first);
        }

        void set_request_size(size_t n) noexcept {
            _allocation_request.first = n;
        }

        void* pointer() const noexcept { return _allocated_object.second; }
        size_t size() const noexcept { return _allocated_object.first; }
    };

    allocation_strategy& _allocator;
    std::vector<allocation> _allocations;
    size_t _position = 0;
    bool _failed = false;
private:
    size_t request(size_t n, allocation_strategy::migrate_fn migrate) noexcept {
        auto id = _allocations.size();
        try {
            _allocations.emplace_back(n, migrate);
        } catch (...) {
            _failed = true;
        }
        return id;
    }
    void set_request_size(size_t id, size_t n) noexcept {
        if (__builtin_expect(!_failed, true)) {
            _allocations[id].set_request_size(n);
        }
    }
    uint8_t* next_object() noexcept {
        return static_cast<uint8_t*>(_allocations[_position++].pointer());
    }
public:
    class sizer {
        object_allocator& _parent;
    public:
        class continuation {
            object_allocator& _parent;
            size_t _idx;
        public:
            continuation(object_allocator& parent, size_t idx) noexcept
                : _parent(parent), _idx(idx) { }
            uint8_t* run(size_t size) noexcept {
                _parent.set_request_size(_idx, size);
                return nullptr;
            }
        };
    public:
        explicit sizer(object_allocator& parent) noexcept
            : _parent(parent) { }

        /// Request allocation of an IMR object
        ///
        /// This method request an allocation of an IMR object of type T. The
        /// arguments are passed to `T::size_when_serialized`.
        ///
        /// \return null pointer of type `uint8_t*`.
        template<typename T, typename MigrateFn, typename... Args>
        uint8_t* allocate(MigrateFn* migrate_fn, Args&&... args) noexcept {
            static_assert(std::is_same_v<typename MigrateFn::structure, T>);
            return do_allocate<T>(migrate_fn, std::forward<Args>(args)...);
        }

        template<typename T, typename MigrateFn, typename... Args>
        auto allocate_nested(MigrateFn* migrate_fn, Args&&... args) noexcept {
            static_assert(std::is_same_v<typename MigrateFn::structure, T>);
            return do_allocate_nested<T>(migrate_fn, std::forward<Args>(args)...);
        }

    private:
        template<typename T, typename... Args>
        uint8_t* do_allocate(migrate_fn_type* migrate_fn, Args&&... args) noexcept {
            auto size = T::size_when_serialized(std::forward<Args>(args)...);
            _parent.request(size, migrate_fn);

            // We are in the sizing phase and only collect information about
            // the size of the required objects. The serializer will return
            // the real pointer to the memory buffer requested here, but since
            // both sizer and serializer need to expose the same interface we
            // need to return something from sizer as well even though the
            // value will be ignored.
            return nullptr;
        }

        template<typename T, typename... Args>
        auto do_allocate_nested(migrate_fn_type* migrate_fn, Args&& ... args) noexcept {
            auto n = _parent.request(0, migrate_fn);
            return T::get_sizer(continuation(_parent, n),
                                std::forward<Args>(args)...);
        }
    };

    class serializer {
        object_allocator& _parent;
    public:
        class continuation {
            uint8_t* _ptr;
        public:
            explicit continuation(uint8_t* ptr) noexcept : _ptr(ptr) { }
            uint8_t* run(uint8_t*) noexcept {
                return _ptr;
            }
        };
    public:
        explicit serializer(object_allocator& parent) noexcept
            : _parent(parent) { }

        /// Writes an IMR object to the preallocated buffer
        ///
        /// In the second serialisation phase this method writes an IMR object
        /// to the buffer requested in the sizing phase. Arguments are passed
        /// to `T::serialize`.
        /// \return pointer to the IMR object
        template<typename T, typename MigrateFn, typename... Args>
        uint8_t* allocate(MigrateFn* migrate_fn, Args&&... args) noexcept {
            static_assert(std::is_same_v<typename MigrateFn::structure, T>);
            return do_allocate<T>(migrate_fn, std::forward<Args>(args)...);
        }

        template<typename T, typename MigrateFn, typename... Args>
        auto allocate_nested(MigrateFn* migrate_fn, Args&&... args) noexcept {
            static_assert(std::is_same_v<typename MigrateFn::structure, T>);
            return do_allocate_nested<T>(migrate_fn, std::forward<Args>(args)...);
        }

    private:
        template<typename T, typename... Args>
        uint8_t* do_allocate(migrate_fn_type* migrate_fn, Args&&... args) noexcept {
            auto ptr = _parent.next_object();
            T::serialize(ptr, std::forward<Args>(args)...);
            return ptr;
        }

        template<typename T, typename... Args>
        auto do_allocate_nested(migrate_fn_type*, Args&& ... args) noexcept {
            auto ptr = _parent.next_object();
            return T::get_serializer(ptr,
                                     continuation(ptr),
                                     std::forward<Args>(args)...);
        }
    };

public:
    explicit object_allocator(allocation_strategy& allocator = current_allocator())
        : _allocator(allocator) { }

    size_t requested_allocations_count() const noexcept { return _allocations.size(); }

    /// Allocates all buffers requested in the sizing phase.
    void allocate_all() {
        if (__builtin_expect(_failed, false)) {
            throw std::bad_alloc();
        }
        auto it = _allocations.begin();
        try {
            // TODO: Send a batch of allocations to the allocation strategy.
            while (it != _allocations.end()) {
                it->allocate(_allocator);
                ++it;
            }
        } catch (...) {
            while (it != _allocations.begin()) {
                --it;
                it->free(_allocator);
            }
            throw;
        }
    }

    sizer get_sizer() noexcept { return sizer(*this); }
    serializer get_serializer() noexcept { return serializer(*this); }
};

}
}
