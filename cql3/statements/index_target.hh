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
#include "cql3/column_identifier.hh"
#include <variant>
#include <boost/regex.hpp>

namespace cql3 {

namespace statements {

struct index_target {
    static const sstring target_option_name;
    static const sstring custom_index_option_name;
    static const boost::regex target_regex;

    enum class target_type {
        regular_values, collection_values, keys, keys_and_values, full
    };

    using single_column = ::shared_ptr<column_identifier>;
    using multiple_columns = std::vector<::shared_ptr<column_identifier>>;
    using value_type = std::variant<single_column, multiple_columns>;

    const value_type value;
    target_type type;

    index_target(::shared_ptr<column_identifier> c, target_type t) : value(c) , type(t) {}
    index_target(std::vector<::shared_ptr<column_identifier>> c, target_type t) : value(std::move(c)), type(t) {}

    sstring column_name() const;

    static target_type from_column_definition(const column_definition& cd);
    // Parses index_target::target_type from it's textual form.
    // e.g. from_sstring("keys") == index_target::target_type::keys
    static index_target::target_type from_sstring(const sstring& s);
    // Parses index_target::target_type from index target string form.
    // e.g. from_target_string("keys(some_column)") == index_target::target_type::keys
    static index_target::target_type from_target_string(const sstring& s);
    // Parses column name from index target string form
    // e.g. column_name_from_target_string("keys(some_column)") == "some_column"
    static sstring column_name_from_target_string(const sstring& s);

    // A CQL column's name may contain any characters. If we use this string
    // as-is inside a target string, it may confuse us when we later try to
    // parse the resulting string (e.g., see issue #10707). We should
    // therefore use the function escape_target_column() to "escape" the
    // target column name, and the reverse function unescape_target_column().
    static sstring escape_target_column(const cql3::column_identifier& col);
    static sstring unescape_target_column(std::string_view str);

    class raw {
    public:
        using single_column = ::shared_ptr<column_identifier::raw>;
        using multiple_columns = std::vector<::shared_ptr<column_identifier::raw>>;
        using value_type = std::variant<single_column, multiple_columns>;

        const value_type value;
        const target_type type;

        raw(::shared_ptr<column_identifier::raw> c, target_type t) : value(c), type(t) {}
        raw(std::vector<::shared_ptr<column_identifier::raw>> pk_columns, target_type t) : value(pk_columns), type(t) {}

        static ::shared_ptr<raw> regular_values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> collection_values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> keys_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> keys_and_values_of(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> full_collection(::shared_ptr<column_identifier::raw> c);
        static ::shared_ptr<raw> columns(std::vector<::shared_ptr<column_identifier::raw>> c);
        ::shared_ptr<index_target> prepare(const schema&) const;
    };
};

sstring to_sstring(index_target::target_type type);

}
}
