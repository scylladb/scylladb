/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <ostream>
#include <seastar/core/sstring.hh>

namespace replica {
class database;
}

namespace data_dictionary {

/**
 * `keyspace_element` is a common interface used to describe elements of keyspace. 
 * It is used in `describe_statement`.
 *
 * Currently the elements of keyspace are:
 * - keyspace
 * - user-defined types
 * - user-defined functions/aggregates
 * - tables, views and indexes
*/
class keyspace_element {
public:
    virtual seastar::sstring keypace_name() const = 0;
    virtual seastar::sstring element_name() const = 0;

    // Override one of these element_type() overloads.
    virtual seastar::sstring element_type() const { return ""; }
    virtual seastar::sstring element_type(replica::database& db) const { return element_type(); }

    // Override one of these describe() overloads.
    virtual std::ostream& describe(std::ostream& os) const { return os; }
    virtual std::ostream& describe(replica::database& db, std::ostream& os, bool with_internals) const { return describe(os); }
};

}
