/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <fmt/core.h>

namespace cql3 {

namespace statements {

class statement_type final {
    enum class type : size_t {
        insert = 0,
        update,
        del,
        select,

        last  // Keep me as last entry
    };
    const type _type;

    statement_type(type t) : _type(t) {
    }
public:
    statement_type() = delete;

    bool is_insert() const {
        return _type == type::insert;
    }
    bool is_update() const {
        return _type == type::update;
    }
    bool is_delete() const {
        return _type == type::del;
    }
    bool is_select() const {
        return _type == type::select;
    }

    static const statement_type INSERT;
    static const statement_type UPDATE;
    static const statement_type DELETE;
    static const statement_type SELECT;

    static constexpr size_t MAX_VALUE = size_t(type::last) - 1;

    explicit operator size_t() const {
        return size_t(_type);
    }

    bool operator==(const statement_type&) const = default;

    friend fmt::formatter<statement_type>;
};

}
}

template <> struct fmt::formatter<cql3::statements::statement_type> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const cql3::statements::statement_type& t, fmt::format_context& ctx) const {
        std::string_view name;
        switch (t._type) {
            using enum cql3::statements::statement_type::type;
            case insert:
                name = "INSERT";
                break;
            case update:
                name = "UPDATE";
                break;
            case del:
                name = "DELETE";
                break;
            case select:
                name = "SELECT";
                break;
            case last:
                name = "LAST";
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};
