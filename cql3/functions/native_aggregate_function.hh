/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "types/types.hh"
#include "native_function.hh"
#include "aggregate_function.hh"
#include <seastar/core/shared_ptr.hh>

namespace cql3 {
namespace functions {

/**
 * Base class for the <code>AggregateFunction</code> native classes.
 */
class native_aggregate_function : public native_function, public aggregate_function {
protected:
    native_aggregate_function(sstring name, data_type return_type,
            std::vector<data_type> arg_types)
        : native_function(std::move(name), std::move(return_type), std::move(arg_types)) {
    }

public:
    virtual bool is_aggregate() const override final {
        return true;
    }
};

}
}
