/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "types/types.hh"
#include <vector>
#include <optional>
#include <fmt/ostream.h>

namespace db {
namespace functions {

class function_name;

class function {
public:
    using opt_bytes = std::optional<bytes>;
    virtual ~function() {}
    virtual const function_name& name() const = 0;
    virtual const std::vector<data_type>& arg_types() const = 0;
    virtual const data_type& return_type() const = 0;

    /**
     * Checks whether the function is a pure function (as in doesn't depend on, nor produce side effects) or not.
     *
     * @return <code>true</code> if the function is a pure function, <code>false</code> otherwise.
     */
    virtual bool is_pure() const = 0;

    /**
     * Checks whether the function is a native/hard coded one or not.
     *
     * @return <code>true</code> if the function is a native/hard coded one, <code>false</code> otherwise.
     */
    virtual bool is_native() const = 0;

    virtual bool requires_thread() const = 0;

    /**
     * Checks whether the function is an aggregate function or not.
     *
     * @return <code>true</code> if the function is an aggregate function, <code>false</code> otherwise.
     */
    virtual bool is_aggregate() const = 0;

    virtual void print(std::ostream& os) const = 0;

    /**
     * Returns the name of the function to use within a ResultSet.
     *
     * @param column_names the names of the columns used to call the function
     * @return the name of the function to use within a ResultSet
     */
    virtual sstring column_name(const std::vector<sstring>& column_names) const = 0;

    friend class function_call;
    friend std::ostream& operator<<(std::ostream& os, const function& f);
};

inline
std::ostream&
operator<<(std::ostream& os, const function& f) {
    f.print(os);
    return os;
}

}
}

template <std::derived_from<db::functions::function> T> struct fmt::formatter<T> : fmt::ostream_formatter {};
