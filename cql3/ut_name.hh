/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"
#include "bytes.hh"

#include <optional>

namespace cql3 {

class column_identifier;

class ut_name final {
    std::optional<sstring> _ks_name;
    ::shared_ptr<column_identifier> _ut_name;
public:
    ut_name(shared_ptr<column_identifier> ks_name, ::shared_ptr<column_identifier> ut_name);

    bool has_keyspace() const;

    void set_keyspace(sstring keyspace);

    const sstring& get_keyspace() const;

    bytes get_user_type_name() const;

    sstring get_string_type_name() const;

    sstring to_cql_string() const;

    friend std::ostream& operator<<(std::ostream& os, const ut_name& n) {
        return os << n.to_cql_string();
    }
};

}
