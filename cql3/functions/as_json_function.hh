/*
 * Copyright (C) 2018-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/functions/scalar_function.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/cql3_type.hh"
#include "cql3/type_json.hh"

#include "bytes_ostream.hh"
#include "types/types.hh"

#include <boost/algorithm/cxx11/any_of.hpp>

namespace cql3 {

namespace functions {

/*
 * This function is used for handling 'SELECT JSON' statement.
 * 'SELECT JSON' is supposed to return a single column named '[json]'
 * with JSON representation of the query result as its value.
 * In order to achieve it, selectors from 'SELECT' are wrapped with
 * 'as_json' function, which also keeps information about underlying
 * selector names and types. This function is not registered in functions.cc,
 * because it should not be invoked directly from CQL.
 * Case-sensitive column names are wrapped in additional quotes,
 * as stated in CQL-JSON documentation.
 */
class as_json_function : public scalar_function {
    std::vector<sstring> _selector_names;
    std::vector<data_type> _selector_types;
public:
    as_json_function(std::vector<sstring>&& selector_names, std::vector<data_type> selector_types)
        : _selector_names(std::move(selector_names)), _selector_types(std::move(selector_types)) {
    }

    virtual bool requires_thread() const override;

    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override {
        bytes_ostream encoded_row;
        encoded_row.write("{", 1);
        for (size_t i = 0; i < _selector_names.size(); ++i) {
            if (i > 0) {
                encoded_row.write(", ", 2);
            }
            bool has_any_upper = boost::algorithm::any_of(_selector_names[i], [](unsigned char c) { return std::isupper(c); });
            encoded_row.write("\"", 1);
            if (has_any_upper) {
                encoded_row.write("\\\"", 2);
            }
            encoded_row.write(_selector_names[i].c_str(), _selector_names[i].size());
            if (has_any_upper) {
                encoded_row.write("\\\"", 2);
            }
            encoded_row.write("\": ", 3);
            sstring row_sstring = to_json_string(*_selector_types[i], parameters[i]);
            encoded_row.write(row_sstring.c_str(), row_sstring.size());
        }
        encoded_row.write("}", 1);
        return bytes(encoded_row.linearize());
    }

    virtual const function_name& name() const override {
        static const function_name f_name = function_name::native_function("as_json");
        return f_name;
    }

    virtual const std::vector<data_type>& arg_types() const override {
        return _selector_types;
    }

    virtual const data_type& return_type() const override {
        return utf8_type;
    }

    virtual bool is_pure() const override {
        return true;
    }

    virtual bool is_native() const override {
        return true;
    }

    virtual bool is_aggregate() const override {
        // Aggregates of aggregates are currently not supported, but JSON handles them
        return false;
    }

    virtual void print(std::ostream& os) const override {
        os << "as_json(";
        bool first = true;
        for (const sstring&  selector_name: _selector_names) {
            if (first) {
                first = false;
            } else {
                os << ", ";
            }
            os << selector_name;
        }
        os << ") -> " << utf8_type->as_cql3_type().to_string();
    }

    virtual sstring column_name(const std::vector<sstring>& column_names) const override {
        return "[json]";
    }

};

}

}
