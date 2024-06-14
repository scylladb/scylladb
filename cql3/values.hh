/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "types/types.hh"
#include "types/collection.hh"
#include "bytes.hh"

#include <optional>
#include <variant>
#include <fmt/core.h>

#include <seastar/util/variant_utils.hh>

#include "utils/fragmented_temporary_buffer.hh"
#include "utils/overloaded_functor.hh"

namespace cql3 {

struct null_value {
    friend bool operator==(const null_value&, const null_value) { return true; }
};

class raw_value;
/// \brief View to a raw CQL protocol value.
///
/// \see raw_value
class raw_value_view {
    std::variant<fragmented_temporary_buffer::view, managed_bytes_view, null_value> _data;
    // Temporary storage is only useful if a raw_value_view needs to be instantiated
    // with a value which lifetime is bounded only to the view itself.
    // This hack is introduced in order to avoid storing temporary storage
    // in an external container, which may cause memory leaking problems.
    // This pointer is disengaged for regular raw_value_view instances.
    // Data is stored in a shared pointer for two reasons:
    // - pointers are cheap to copy
    // - it makes the view keep its semantics - it's safe to copy a view multiple times
    //   and all copies still refer to the same underlying data.
    lw_shared_ptr<managed_bytes> _temporary_storage = nullptr;

    raw_value_view(null_value data)
        : _data{std::move(data)}
    {}
    raw_value_view(fragmented_temporary_buffer::view data)
        : _data{data}
    {}
    raw_value_view(managed_bytes_view data)
        : _data{data}
    {}
    // This constructor is only used by make_temporary() and it acquires ownership
    // of the given buffer. The view created that way refers to its own temporary storage.
    explicit raw_value_view(managed_bytes&& temporary_storage);
public:
    static raw_value_view make_null() {
        return raw_value_view{null_value{}};
    }
    static raw_value_view make_value(fragmented_temporary_buffer::view view) {
        return raw_value_view{view};
    }
    static raw_value_view make_value(managed_bytes_view view) {
        return raw_value_view{view};
    }
    static raw_value_view make_value(bytes_view view) {
        return raw_value_view{managed_bytes_view(view)};
    }
    static raw_value_view make_temporary(raw_value&& value);
    bool is_null() const {
        return std::holds_alternative<null_value>(_data);
    }
    // An empty value is not null, but it has 0 bytes of data.
    // An empty int value can be created in CQL using blobasint(0x).
    bool is_empty_value() const {
        if (is_null()) {
            return false;
        }
        return size_bytes() == 0;
    }
    bool is_value() const {
        return _data.index() <= 1;
    }
    explicit operator bool() const {
        return is_value();
    }

    template <typename Func>
    requires std::invocable<Func, const managed_bytes_view&> && std::invocable<Func, const fragmented_temporary_buffer::view&>
    decltype(auto) with_value(Func f) const {
        switch (_data.index()) {
        case 0: return f(std::get<fragmented_temporary_buffer::view>(_data));
        default: return f(std::get<managed_bytes_view>(_data));
        }
    }

    template <typename Func>
    requires std::invocable<Func, bytes_view>
    decltype(auto) with_linearized(Func f) const {
        return with_value([&] (const FragmentedView auto& v) {
            return ::with_linearized(v, std::forward<Func>(f));
        });
    }

    size_t size_bytes() const {
        return with_value([&] (const FragmentedView auto& v) {
            return v.size_bytes();
        });
    }

    template <typename ValueType>
    ValueType deserialize(const abstract_type& t) const {
        return value_cast<ValueType>(with_value([&] (const FragmentedView auto& v) { return t.deserialize(v); }));
    }

    template <typename ValueType>
    ValueType deserialize(const collection_type_impl& t) const {
        return value_cast<ValueType>(with_value([&] (const FragmentedView auto& v) { return t.deserialize(v); }));
    }

    void validate(const abstract_type& t) const {
        return with_value([&] (const FragmentedView auto& v) { return t.validate(v); });
    }

    template <typename ValueType>
    ValueType validate_and_deserialize(const collection_type_impl& t) const {
        return with_value([&] (const FragmentedView auto& v) {
            t.validate(v);
            return value_cast<ValueType>(t.deserialize(v));
        });
    }

    template <typename ValueType>
    ValueType validate_and_deserialize(const abstract_type& t) const {
        return with_value([&] (const FragmentedView auto& v) {
            t.validate(v);
            return value_cast<ValueType>(t.deserialize(v));
        });
    }

    friend managed_bytes_opt to_managed_bytes_opt(const cql3::raw_value_view& view) {
        if (view.is_value()) {
            return view.with_value([] (const FragmentedView auto& v) { return managed_bytes(v); });
        }
        return managed_bytes_opt();
    }

    friend managed_bytes_opt to_managed_bytes_opt(cql3::raw_value_view&& view) {
        if (view._temporary_storage) {
            return std::move(*view._temporary_storage);
        }
        return to_managed_bytes_opt(view);
    }

    friend fmt::formatter<raw_value_view>;
    friend class raw_value;
};

/// \brief Raw CQL protocol value.
///
/// The `raw_value` type represents an uninterpreted value from the CQL wire
/// protocol. A raw value can hold either a null value, or a byte
/// blob that represents the value.
class raw_value {
    std::variant<bytes, managed_bytes, null_value> _data;

    raw_value(null_value&& data)
        : _data{std::move(data)}
    {}
    raw_value(bytes&& data)
        : _data{std::move(data)}
    {}
    raw_value(const bytes& data)
        : _data{data}
    {}
    raw_value(managed_bytes&& data)
        : _data{std::move(data)}
    {}
    raw_value(const managed_bytes& data)
        : _data{data}
    {}
public:
    static raw_value make_null() {
        return raw_value{null_value{}};
    }
    static raw_value make_value(const raw_value_view& view);
    static raw_value make_value(managed_bytes&& mb) {
        return raw_value{std::move(mb)};
    }
    static raw_value make_value(managed_bytes_opt&& mbo) {
        return mbo ? make_value(std::move(*mbo)) : make_null();
    }
    static raw_value make_value(const managed_bytes& mb) {
        return raw_value{mb};
    }
    static raw_value make_value(const managed_bytes_opt& mbo) {
        if (mbo) {
            return make_value(*mbo);
        }
        return make_null();
    }
    static raw_value make_value(bytes&& bytes) {
        return raw_value{std::move(bytes)};
    }
    static raw_value make_value(const bytes& bytes) {
        return raw_value{bytes};
    }
    static raw_value make_value(const bytes_opt& bytes) {
        if (bytes) {
            return make_value(*bytes);
        }
        return make_null();
    }
    bool is_null() const {
        return std::holds_alternative<null_value>(_data);
    }
    // An empty value is not null, but it has 0 bytes of data.
    // An empty int value can be created in CQL using blobasint(0x).
    bool is_empty_value() const {
        if (is_null()) {
            return false;
        }
        return view().size_bytes() == 0;
    }
    bool is_value() const {
        return _data.index() <= 1;
    }
    explicit operator bool() const {
        return is_value();
    }
    bytes to_bytes() && {
        return std::visit(overloaded_functor{
            [](bytes&& bytes_val) { return std::move(bytes_val); },
            [](managed_bytes&& managed_bytes_val) { return ::to_bytes(managed_bytes_val); },
            [](null_value&&) -> bytes {
                throw std::runtime_error("to_bytes() called on raw value that is null");
            },
        }, std::move(_data));
    }
    bytes_opt to_bytes_opt() && {
        return std::visit(overloaded_functor{
            [](bytes&& bytes_val) { return bytes_opt(bytes_val); },
            [](managed_bytes&& managed_bytes_val) { return bytes_opt(::to_bytes(managed_bytes_val)); },
            [](null_value&&) -> bytes_opt {
                return std::nullopt;
            },
        }, std::move(_data));
    }
    managed_bytes to_managed_bytes() && {
        return std::visit(overloaded_functor{
            [](bytes&& bytes_val) { return managed_bytes(bytes_val); },
            [](managed_bytes&& managed_bytes_val) { return std::move(managed_bytes_val); },
            [](null_value&&) -> managed_bytes {
                throw std::runtime_error("to_managed_bytes() called on raw value that is null");
            },
        }, std::move(_data));
    }
    managed_bytes_opt to_managed_bytes_opt() && {
        return std::visit(overloaded_functor{
            [](bytes&& bytes_val) { return managed_bytes_opt(bytes_val); },
            [](managed_bytes&& managed_bytes_val) { return managed_bytes_opt(std::move(managed_bytes_val)); },
            [](null_value&&) -> managed_bytes_opt {
                return std::nullopt;
            },
        }, std::move(_data));
    }
    raw_value_view view() const;
    friend class raw_value_view;

    friend bool operator==(const raw_value& v1, const raw_value& v2);
};

}

inline bytes to_bytes(const cql3::raw_value_view& view)
{
    return view.with_value([] (const FragmentedView auto& v) {
        return linearized(v);
    });
}

inline bytes_opt to_bytes_opt(const cql3::raw_value_view& view) {
    if (view.is_value()) {
        return to_bytes(view);
    }
    return bytes_opt();
}

inline bytes_opt to_bytes_opt(const cql3::raw_value& value) {
    return to_bytes_opt(value.view());
}

template <> struct fmt::formatter<cql3::raw_value_view> : fmt::formatter<string_view> {
    auto format(const cql3::raw_value_view& value, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <> struct fmt::formatter<cql3::raw_value> : fmt::formatter<string_view> {
    auto format(const cql3::raw_value& value, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", value.view());
    }
};
