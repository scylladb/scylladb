/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "function.hh"
#include "types/types.hh"
#include "cql3/cql3_type.hh"
#include "cql3/functions/function_name.hh"
#include <vector>
#include <boost/functional/hash.hpp>
#include <fmt/core.h>


template <> struct fmt::formatter<std::vector<data_type>> : fmt::formatter<string_view> {
    auto format(const std::vector<data_type>& arg_types, fmt::format_context& ctx) const -> decltype(ctx.out());
};

namespace cql3 {

namespace functions {

/**
 * Base class for our native/hardcoded functions.
 */
class abstract_function : public virtual function {
protected:
    function_name _name;
    std::vector<data_type> _arg_types;
    data_type _return_type;

    abstract_function(function_name name, std::vector<data_type> arg_types, data_type return_type)
            : _name(std::move(name)), _arg_types(std::move(arg_types)), _return_type(std::move(return_type)) {
    }

public:

    virtual bool requires_thread() const override;

    virtual const function_name& name() const override {
        return _name;
    }

    virtual const std::vector<data_type>&  arg_types() const override {
        return _arg_types;
    }

    virtual const data_type& return_type() const override {
        return _return_type;
    }

    bool operator==(const abstract_function& x) const {
        return _name == x._name
            && _arg_types == x._arg_types
            && _return_type == x._return_type;
    }

    virtual sstring column_name(const std::vector<sstring>& column_names) const override {
        return format("{}({})", _name, fmt::join(column_names, ", "));
    }

    virtual void print(std::ostream& os) const override;
};

inline
void
abstract_function::print(std::ostream& os) const {
    fmt::print(os, "{} : ({}) -> {}",
               _name, _arg_types, _return_type->as_cql3_type().to_string());
}

}
}

namespace std {

template <>
struct hash<cql3::functions::abstract_function> {
    size_t operator()(const cql3::functions::abstract_function& f) const {
        using namespace cql3::functions;
        size_t v = 0;
        boost::hash_combine(v, std::hash<function_name>()(f.name()));
        boost::hash_combine(v, boost::hash_value(f.arg_types()));
        // FIXME: type hash
        //boost::hash_combine(v, std::hash<shared_ptr<abstract_type>>()(f.return_type()));
        return v;
    }
};

}
