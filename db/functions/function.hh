/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
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

    /**
     * Checks whether the function is an external one, i.e. whether its value comes from an
     * external search system rather than from evaluating its arguments locally.
     *
     * Statement preparation finds calls to such functions by this capability and either lowers
     * them to a value injected per row or rejects the clause they appear in. Two invariants
     * follow: an external function must be non-pure, so that a call whose arguments happen to be
     * literals is not constant-folded before preparation gets to it, and its own evaluation must
     * never be reached.
     *
     * @return <code>true</code> if the function is an external one, <code>false</code> otherwise.
     */
    virtual bool is_external() const { return false; }

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
