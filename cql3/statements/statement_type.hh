/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <ostream>
#include <fmt/ostream.h>

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

    friend std::ostream &operator<<(std::ostream &os, const statement_type& t) {
        switch (t._type) {
        case type::insert: return os << "INSERT";
        case type::update: return os << "UPDATE";
        case type::del: return os << "DELETE";
        case type::select : return os << "SELECT";

        case type::last : return os << "LAST";
        }
        return os;
    }
};

}
}

template <> struct fmt::formatter<cql3::statements::statement_type> : fmt::ostream_formatter {};
