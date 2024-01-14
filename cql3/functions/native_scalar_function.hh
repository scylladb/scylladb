/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "native_function.hh"
#include "scalar_function.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

namespace cql3 {
namespace functions {

/**
 * Base class for the <code>ScalarFunction</code> native classes.
 */
class native_scalar_function : public native_function, public scalar_function {
protected:
    native_scalar_function(sstring name, data_type return_type, std::vector<data_type> args_type)
            : native_function(std::move(name), std::move(return_type), std::move(args_type)) {
    }

public:
    virtual bool is_aggregate() const override {
        return false;
    }
};

template <typename Func, bool Pure>
class native_scalar_function_for : public native_scalar_function {
    Func _func;
public:
    native_scalar_function_for(sstring name,
                               data_type return_type,
                               const std::vector<data_type> arg_types,
                               Func&& func)
            : native_scalar_function(std::move(name), std::move(return_type), std::move(arg_types))
            , _func(std::forward<Func>(func)) {
    }
    virtual bool is_pure() const override {
        return Pure;
    }
    virtual bytes_opt execute(std::span<const bytes_opt> parameters) override {
        try {
            return _func(parameters);
        } catch(exceptions::cassandra_exception&) {
            // If the function's code took the time to produce an official
            // cassandra_exception, pass it through. Otherwise, below we will
            // wrap the unknown exception in a function_execution_exception.
            throw;
        } catch(...) {
            std::vector<sstring> args;
            args.reserve(arg_types().size());
            for (const data_type& a : arg_types()) {
                args.push_back(a->name());
            }
            throw exceptions::function_execution_exception(name().name,
                format("Failed execution of function {}: {}", name(), std::current_exception()), name().keyspace, std::move(args));
        }
    }
};

template <bool Pure, typename Func>
shared_ptr<function>
make_native_scalar_function(sstring name,
                            data_type return_type,
                            std::vector<data_type> args_type,
                            Func&& func) {
    return ::make_shared<native_scalar_function_for<Func, Pure>>(std::move(name),
                                                  std::move(return_type),
                                                  std::move(args_type),
                                                  std::forward<Func>(func));
}

}
}
