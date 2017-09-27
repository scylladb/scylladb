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

#include "imr/core.hh"

namespace imr {

/// Optionally present object
///
/// Represents a value that may be not present. Information whether or not
/// the optional is engaged is not stored and must be provided by external
/// context.
template<typename Tag, typename Type>
struct optional {
    using underlying = Type;
public:
    template<::mutable_view is_mutable>
    class basic_view {
        using pointer_type = std::conditional_t<is_mutable == ::mutable_view::no,
                                                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
    public:
        explicit basic_view(pointer_type ptr) noexcept : _ptr(ptr) { }

        operator basic_view<::mutable_view::no>() const noexcept {
            return basic_view<::mutable_view::no>(_ptr);
        }

        template<typename Context = no_context_t>
        GCC6_CONCEPT(requires requires(const Context& ctx) {
            { ctx.template context_for<Tag>() } noexcept;
        })
        auto get(const Context& ctx = no_context) noexcept {
            return Type::make_view(_ptr, ctx.template context_for<Tag>(_ptr));
        }
    };

    using view = basic_view<::mutable_view::no>;
    using mutable_view = basic_view<::mutable_view::yes>;
public:
    template<typename Context = no_context_t>
    static auto make_view(const uint8_t* in, const Context& ctx = no_context) noexcept {
        return view(in);
    }
    template<typename Context = no_context_t>
    static auto make_view(uint8_t* in, const Context& ctx = no_context) noexcept {
        return mutable_view(in);
    }
public:
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template is_present<Tag>() } noexcept -> bool;
    })
    static size_t serialized_object_size(const uint8_t* in, const Context& context) noexcept {
        return context.template is_present<Tag>()
               ? Type::serialized_object_size(in, context)
               : 0;
    }

    template<typename... Args>
    static size_t size_when_serialized(Args&&... args) noexcept {
        return Type::size_when_serialized(std::forward<Args>(args)...);
    }

    template<typename... Args>
    static size_t serialize(uint8_t* out, Args&&... args) noexcept {
        return Type::serialize(out, std::forward<Args>(args)...);
    }

    template<typename Continuation = no_op_continuation>
    static auto get_sizer(Continuation cont = no_op_continuation()) {
        return Type::get_sizer(std::move(cont));
    }

    template<typename Continuation = no_op_continuation>
    static auto get_serializer(uint8_t* out, Continuation cont = no_op_continuation()) {
        return Type::get_serializer(out, std::move(cont));
    }

};

}
