/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <boost/regex.hpp>
#include <stdexcept>
#include "index_target.hh"
#include "index/secondary_index.hh"
#include <boost/algorithm/string/join.hpp>

namespace cql3 {

namespace statements {

using db::index::secondary_index;

const sstring index_target::target_option_name = "target";
const sstring index_target::custom_index_option_name = "class_name";
const boost::regex index_target::target_regex("^(keys|entries|values|full)\\((.+)\\)$");

sstring index_target::column_name() const {
    struct as_string_visitor {
        const index_target* target;
        sstring operator()(const std::vector<::shared_ptr<column_identifier>>& columns) const {
            return "(" + boost::algorithm::join(columns | boost::adaptors::transformed(
                    [](const ::shared_ptr<cql3::column_identifier>& ident) -> sstring {
                        return ident->to_string();
                    }), ",") + ")";
        }

        sstring operator()(const ::shared_ptr<column_identifier>& column) const {
            return column->to_string();
        }
    };

    return std::visit(as_string_visitor {this}, value);
}

index_target::target_type index_target::from_sstring(const sstring& s)
{
    if (s == "keys") {
        return index_target::target_type::keys;
    } else if (s == "entries") {
        return index_target::target_type::keys_and_values;
    } else if (s == "regular_values") {
        return index_target::target_type::regular_values;
    } else if (s == "values") {
        return index_target::target_type::collection_values;
    } else if (s == "full") {
        return index_target::target_type::full;
    }
    throw std::runtime_error(format("Unknown target type: {}", s));
}

index_target::target_type index_target::from_target_string(const sstring& target) {
    boost::cmatch match;
    if (boost::regex_match(target.data(), match, target_regex)) {
        return index_target::from_sstring(match[1].str());
    }
    return target_type::regular_values;
}

// A CQL column's name may contain any characters. If we use this string as-is
// inside a target string, it may confuse us when we later try to parse the
// resulting string (e.g., see issue #10707). We should therefore use the
// function escape_target_column() to "escape" the target column name, and use
// the reverse function unescape_target_column() to undo this.
// Cassandra uses for this escaping the CQL syntax of this column name
// (basically, column_identifier::as_cql_name()). This is an overkill,
// but since we already have such code, we might as well use it.
sstring index_target::escape_target_column(const cql3::column_identifier& col) {
    return col.to_cql_string();
}
sstring index_target::unescape_target_column(std::string_view str) {
    // We don't have a reverse version of util::maybe_quote(), so
    // we need to open-code it here. Cassandra has this too - in
    // index/TargetParser.java
    if (str.size() >= 2 && str.starts_with('"') && str.ends_with('"')) {
        str.remove_prefix(1);
        str.remove_suffix(1);
        // remove doubled quotes in the middle of the string, which to_cql_string()
        // adds. This code is inefficient but rarely called so it's fine.
        static const boost::regex double_quote_re("\"\"");
        return boost::regex_replace(std::string(str), double_quote_re, "\"");
    }
    return sstring(str);
}

sstring index_target::column_name_from_target_string(const sstring& target) {
    boost::cmatch match;
    if (boost::regex_match(target.data(), match, target_regex)) {
        return unescape_target_column(match[2].str());
    }
    return unescape_target_column(target);
}

::shared_ptr<index_target::raw>
index_target::raw::regular_values_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::regular_values);
}

::shared_ptr<index_target::raw>
index_target::raw::collection_values_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::collection_values);
}

::shared_ptr<index_target::raw>
index_target::raw::keys_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::keys);
}

::shared_ptr<index_target::raw>
index_target::raw::keys_and_values_of(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::keys_and_values);
}

::shared_ptr<index_target::raw>
index_target::raw::full_collection(::shared_ptr<column_identifier::raw> c) {
    return ::make_shared<raw>(c, target_type::full);
}

::shared_ptr<index_target::raw>
index_target::raw::columns(std::vector<::shared_ptr<column_identifier::raw>> c) {
    return ::make_shared<raw>(std::move(c), target_type::regular_values);
}

::shared_ptr<index_target>
index_target::raw::prepare(const schema& s) const {
    struct prepare_visitor {
        const schema& _schema;
        target_type _type;

        ::shared_ptr<index_target> operator()(const std::vector<::shared_ptr<column_identifier::raw>>& columns) const {
            auto prepared_idents = boost::copy_range<std::vector<::shared_ptr<column_identifier>>>(
                    columns | boost::adaptors::transformed([this] (const ::shared_ptr<column_identifier::raw>& raw_ident) {
                        return raw_ident->prepare_column_identifier(_schema);
                    })
            );
            return ::make_shared<index_target>(std::move(prepared_idents), _type);
        }

        ::shared_ptr<index_target> operator()(::shared_ptr<column_identifier::raw> raw_ident) const {
            return ::make_shared<index_target>(raw_ident->prepare_column_identifier(_schema), _type);
        }
    };

    return std::visit(prepare_visitor{s, type}, value);
}

sstring to_sstring(index_target::target_type type)
{
    switch (type) {
    case index_target::target_type::keys: return "keys";
    case index_target::target_type::keys_and_values: return "entries";
    case index_target::target_type::regular_values: return "regular_values";
    case index_target::target_type::collection_values: return "values";
    case index_target::target_type::full: return "full";
    }
    throw std::runtime_error("to_sstring(index_target::target_type): should not reach");
}

}

}
