/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "seastarx.hh"

#include <optional>

namespace cql3 {

/**
 * Base class for the names of the keyspace elements (e.g. table, index ...)
 */
class keyspace_element_name {
    /**
     * The keyspace name as stored internally.
     */
    std::optional<sstring> _ks_name = std::nullopt;

public:
    /**
     * Sets the keyspace.
     *
     * @param ks the keyspace name
     * @param keepCase <code>true</code> if the case must be kept, <code>false</code> otherwise.
     */
    void set_keyspace(std::string_view ks, bool keep_case);

    /**
     * Checks if the keyspace is specified.
     * @return <code>true</code> if the keyspace is specified, <code>false</code> otherwise.
     */
    bool has_keyspace() const;

    const sstring& get_keyspace() const;

    virtual sstring to_string() const;

protected:
    /**
     * Converts the specified name into the name used internally.
     *
     * @param name the name
     * @param keepCase <code>true</code> if the case must be kept, <code>false</code> otherwise.
     * @return the name used internally.
     */
    static sstring to_internal_name(std::string_view name, bool keep_case);
};

}
