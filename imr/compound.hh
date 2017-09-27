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

#include <seastar/util/gcc6-concepts.hh>

#include "utils/meta.hh"

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

template<typename Tag, typename Type>
struct member {
    using tag = Tag;
    using type = Type;
};

namespace internal {

template<typename Tag>
struct do_find_member {
    template<typename Member>
    using type = std::is_same<Tag, typename Member::tag>;
};

template<typename Tag, typename... Members>
static constexpr auto get_member_index = meta::find_if<do_find_member<Tag>::template type, Members...>;

template<typename Tag, typename... Members>
using get_member = meta::get<get_member_index<Tag, Members...>, Members...>;

template<size_t Offset, size_t N, template<size_t> typename Function>
struct do_generate_branch_tree {
    template<typename... Args>
    static decltype(auto) run(size_t n, Args&&... args) {
        if constexpr (N == 1) {
            return Function<Offset>::run(std::forward<Args>(args)...);
        } else if (N >= 2) {
        if (n < Offset + N / 2) {
            return do_generate_branch_tree<Offset, N / 2, Function>::run(n, std::forward<Args>(args)...);
        } else {
            return do_generate_branch_tree<Offset + N / 2, N - N / 2, Function>::run(n, std::forward<Args>(args)...);
        }
    }
    }
};

template<size_t N, template<size_t> typename Function>
using generate_branch_tree = do_generate_branch_tree<0, N, Function>;

}

template<typename Tag, typename... Alternatives>
struct variant {
    class alternative_index {
        size_t _index;
    private:
        constexpr explicit alternative_index(size_t idx) noexcept
            : _index(idx) { }

        friend class variant;
    public:
        constexpr size_t index() const noexcept { return _index; }
    };

    template<typename AlternativeTag>
    constexpr static alternative_index index_for() noexcept {
        return alternative_index(internal::get_member_index<AlternativeTag, Alternatives...>);
    }
private:
    template<size_t N>
    struct alternative_visitor {
        template<typename Visitor>
        static decltype(auto) run(Visitor&& visitor) {
            using member = typename meta::get<N, Alternatives...>;
            return visitor(static_cast<member*>(nullptr));
        }
    };

    template<typename Visitor>
    static decltype(auto) choose_alternative(alternative_index index, Visitor&& visitor) {
        // For large sizeof...(Alternatives) a jump table may be the better option.
        return internal::generate_branch_tree<sizeof...(Alternatives), alternative_visitor>::run(index.index(), std::forward<Visitor>(visitor));
    }
public:
    template<::mutable_view is_mutable>
    class basic_view {
        using pointer_type = std::conditional_t<is_mutable == ::mutable_view::no,
                                                const uint8_t*, uint8_t*>;
        pointer_type _ptr;
    public:
        explicit basic_view(pointer_type ptr) noexcept
            : _ptr(ptr)
        { }

        pointer_type raw_pointer() const noexcept { return _ptr; }

        operator basic_view<::mutable_view::no>() const noexcept {
            return basic_view<::mutable_view::no>(_ptr);
        }

        template<typename AlternativeTag, typename Context = no_context_t>
        auto as(const Context& context = no_context) noexcept {
            using member = internal::get_member<AlternativeTag, Alternatives...>;
            return member::type::make_view(_ptr, context.template context_for<AlternativeTag>(_ptr));
        }

        template<typename Visitor, typename Context>
        decltype(auto) visit(Visitor&& visitor, const Context& context) {
            auto alt_idx = context.template active_alternative_of<Tag>();
            return choose_alternative(alt_idx, [&] (auto object) {
                using type = std::remove_pointer_t<decltype(object)>;
                return visitor(type::type::make_view(_ptr, context.template context_for<typename type::tag>(_ptr)));
            });
        }

        template<typename Visitor, typename Context>
        decltype(auto) visit_type(Visitor&& visitor, const Context& context) {
            auto alt_idx = context.template active_alternative_of<Tag>();
            return choose_alternative(alt_idx, [&] (auto object) {
                using type = std::remove_pointer_t<decltype(object)>;
                return visitor(static_cast<type*>(nullptr));
            });
        }
    };

    using view = basic_view<::mutable_view::no>;
    using mutable_view = basic_view<::mutable_view::yes>;

public:
    template<typename Context>
    static view make_view(const uint8_t* in, const Context& context) noexcept {
        return view(in);
    }

    template<typename Context>
    static mutable_view make_view(uint8_t* in, const Context& context) noexcept {
        return mutable_view(in);
    }

public:
    template<typename Context>
    GCC6_CONCEPT(requires requires(const Context& ctx) {
        { ctx.template active_alternative_of<Tag>() } noexcept -> alternative_index;
    })
    static size_t serialized_object_size(const uint8_t* in, const Context& context) noexcept {
        return choose_alternative(context.template active_alternative_of<Tag>(), [&] (auto object) noexcept {
            using alternative = std::remove_pointer_t<decltype(object)>;
            return alternative::type::serialized_object_size(in, context.template context_for<typename alternative::tag>(in));
        });
    }

    template<typename AlternativeTag, typename... Args>
    static size_t size_when_serialized(Args&&... args) noexcept {
        using member = internal::get_member<AlternativeTag, Alternatives...>;
        return member::type::size_when_serialized(std::forward<Args>(args)...);
    }

    template<typename AlternativeTag, typename... Args>
    static size_t serialize(uint8_t* out, Args&&... args) noexcept {
        using member = internal::get_member<AlternativeTag, Alternatives...>;
        return member::type::serialize(out, std::forward<Args>(args)...);
    }

    template<typename AlternativeTag, typename Continuation = no_op_continuation>
    static auto get_sizer(Continuation cont = no_op_continuation()) {
        using member = internal::get_member<AlternativeTag, Alternatives...>;
        return member::type::get_sizer(std::move(cont));
    }

    template<typename AlternativeTag, typename Continuation = no_op_continuation>
    static auto get_serializer(uint8_t* out, Continuation cont = no_op_continuation()) {
        using member = internal::get_member<AlternativeTag, Alternatives...>;
        return member::type::get_serializer(out, std::move(cont));
    }
};

}
