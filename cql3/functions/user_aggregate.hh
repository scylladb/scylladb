/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "scalar_function.hh"
#include "cql3/description.hh"
#include "cql3/functions/function_name.hh"
#include "db/functions/aggregate_function.hh"

namespace cql3 {
namespace functions {

class user_aggregate : public db::functions::aggregate_function {
public:
    user_aggregate(function_name fname, bytes_opt initcond, ::shared_ptr<scalar_function> sfunc, ::shared_ptr<scalar_function> reducefunc, ::shared_ptr<scalar_function> finalfunc);
    bool has_finalfunc() const;

    description describe(with_create_statement) const;

    seastar::shared_ptr<scalar_function> sfunc() const {
        return _agg.aggregation_function;
    }
    seastar::shared_ptr<scalar_function> reducefunc() const {
        return _agg.state_reduction_function;
    }
    seastar::shared_ptr<scalar_function> finalfunc() const {
        return _agg.state_to_result_function;
    }
    const bytes_opt& initcond() const {
        return _agg.initial_state;
    }
};

}
}
