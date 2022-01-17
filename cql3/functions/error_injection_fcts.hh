/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "native_scalar_function.hh"

namespace cql3
{

namespace functions
{

namespace error_injection
{

class failure_injection_function  : public native_scalar_function {
protected:
    failure_injection_function(sstring name, data_type return_type, std::vector<data_type> args_type)
            : native_scalar_function(std::move(name), std::move(return_type), std::move(args_type)) {
    }

    bool requires_thread() const override {
        return true;
    }
};

shared_ptr<function> make_enable_injection_function();
shared_ptr<function> make_disable_injection_function();
shared_ptr<function> make_enabled_injections_function();

} // namespace error_injection

} // namespace functions

} // namespace cql3
