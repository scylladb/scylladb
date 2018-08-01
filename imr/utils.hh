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

#include <type_traits>

#include "imr/core.hh"
#include "imr/alloc.hh"
#include "imr/concepts.hh"

namespace imr {
namespace utils {

class basic_object {
public:
    struct tags {
        class back_pointer { };
        class object { };
    };
protected:
    uint8_t* _data = nullptr;

    friend struct methods::mover<imr::tagged_type<tags::back_pointer, imr::pod<basic_object*>>>;
protected:
    explicit basic_object(uint8_t* ptr) noexcept : _data(ptr) { }

    void set_data(uint8_t* ptr) noexcept { _data = ptr; }
public:
    basic_object() = default;
    basic_object(basic_object&& other) noexcept : _data(std::exchange(other._data, nullptr)) { }
    basic_object(const basic_object&) = delete;
};

template<typename Context, typename... State>
class object_context {
    std::tuple<State...> _state;
private:
    template<size_t... Index>
    Context create(const uint8_t* ptr, std::index_sequence<Index...>) const noexcept {
        return Context(ptr, std::get<Index>(_state)...);
    }
public:
    object_context(const uint8_t*, State... state) : _state { state... } { }
    template<typename Tag, typename... Args>
    auto context_for(const uint8_t* ptr, Args&&... args) const noexcept {
        if constexpr (std::is_same_v<Tag, basic_object::tags::back_pointer>) {
            return no_context_t();
        } else {
            return create(ptr, std::index_sequence_for<State...>());
        }
    }
};

}

namespace methods {

template<>
struct mover<imr::tagged_type<utils::basic_object::tags::back_pointer, imr::pod<utils::basic_object*>>> {
    static void run(uint8_t* ptr, ...) {
        auto bptr = imr::tagged_type<utils::basic_object::tags::back_pointer, imr::pod<utils::basic_object*>>::make_view(ptr).load();
        bptr->_data = ptr;
    }
};

}

namespace utils {

/// Unique pointer to an IMR object
///
/// This is an LSA-aware unique-owner pointer to an IMR object.
template<typename Structure>
class object : public basic_object {
public:
    using structure = imr::structure<
                        imr::member<tags::back_pointer, imr::tagged_type<tags::back_pointer, imr::pod<basic_object*>>>,
                        imr::member<tags::object, Structure>
                      >;
    static constexpr size_t size_overhead = sizeof(basic_object*);
private:
    explicit object(uint8_t* ptr) noexcept
        : basic_object(ptr)
    {
        structure::template get_member<tags::back_pointer>(_data).store(this);
    }
public:
    object() = default;
    object(object&& other) noexcept : basic_object(std::move(other)) {
        if (_data) {
            structure::template get_member<tags::back_pointer>(_data).store(this);
        }
    }

    object& operator=(object&& other) noexcept {
        swap(other);
        return *this;
    }

    ~object() {
        if (_data) {
            imr::methods::destroy<structure>(_data);
            current_allocator().free(_data);
        }
    }

    void swap(object& other) noexcept {
        std::swap(_data, other._data);
        if (_data) {
            structure::template get_member<tags::back_pointer>(_data).store(this);
        }
        if (other._data) {
            structure::template get_member<tags::back_pointer>(other._data).store(&other);
        }
    }

    explicit operator bool() const noexcept { return bool(_data); }

    uint8_t* get() noexcept { return _data ? _data + structure::template offset_of<tags::object>(_data) : nullptr; }
    const uint8_t* get() const noexcept { return _data ? _data + structure::template offset_of<tags::object>(_data) : nullptr; }

    /// Creates an IMR object from a raw writer
    ///
    /// This low-level function creates an IMR object owned by `object` using
    /// a raw writer (i.e. does not necessarily follow the standard IMR
    /// serialisation process). This is useful for fast copying of trivial
    /// IMR objects.
    ///
    /// \note This function could be deprecated once the IMR starts supporting
    /// copying IMR objects.
    template<typename RawWriter>
    GCC6_CONCEPT(requires requires (RawWriter wr, uint8_t* ptr) {
        { wr(ptr) } noexcept;
    })
    static object make_raw(size_t len, RawWriter&& wr, allocation_strategy::migrate_fn migrate = &imr::alloc::default_lsa_migrate_fn<structure>::migrate_fn) {
        object obj;
        auto ptr = static_cast<uint8_t*>(current_allocator().alloc(migrate, sizeof(void*) + len, 1));
        wr(ptr + sizeof(void*));
        auto view = structure::make_view(ptr);
        view.template get<tags::back_pointer>().store(&obj);
        obj.set_data(ptr);
        return obj;
    }

    /// Create an IMR objects
    template<typename Writer, typename MigrateFn>
    GCC6_CONCEPT(requires WriterAllocator<Writer, Structure>)
    static object make(Writer&& object_writer,
                       MigrateFn* migrate = &imr::alloc::default_lsa_migrate_fn<structure>::migrate_fn) {
        static_assert(std::is_same_v<typename MigrateFn::structure, structure>);
        return do_make(std::forward<Writer>(object_writer), migrate);
    }
private:
    template<typename Writer>
    GCC6_CONCEPT(requires WriterAllocator<Writer, Structure>)
    static object do_make(Writer&& object_writer, allocation_strategy::migrate_fn migrate) {
        struct alloc_deleter {
            size_t _size;

            void operator()(uint8_t* ptr) {
                current_allocator().free(ptr, _size);
            }
        };
        using alloc_unique_ptr = std::unique_ptr<uint8_t[], alloc_deleter>;

        auto writer = [&object_writer] (auto&& ser, auto&& alloc) {
            return object_writer(ser.serialize(nullptr).serialize_nested(), alloc).done();
        };

        auto& alloc = current_allocator();
        alloc::object_allocator allocator(alloc);
        auto obj_size = structure::size_when_serialized(writer, allocator.get_sizer());
        auto ptr = alloc_unique_ptr(static_cast<uint8_t*>(alloc.alloc(migrate, obj_size, 1)), alloc_deleter { obj_size });
        allocator.allocate_all();
        structure::serialize(ptr.get(), writer, allocator.get_serializer());
        return object(ptr.release());
    }
};

}

}
