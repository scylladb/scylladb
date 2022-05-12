/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "abstract_function.hh"

namespace cql3 {
namespace functions {

/**
 * Base class for our native/hardcoded functions.
 */
class native_function : public abstract_function {
protected:
    native_function(sstring name, data_type return_type, std::vector<data_type> arg_types)
        : abstract_function(function_name::native_function(std::move(name)),
                std::move(arg_types), std::move(return_type)) {
    }

public:
    // Most of our functions are pure, the other ones should override this
    virtual bool is_pure() const override {
        return true;
    }

    virtual bool is_native() const override {
        return true;
    }
};

}
}
