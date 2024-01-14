/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "scalar_function.hh"
#include "data_dictionary/keyspace_element.hh"
#include "cql3/functions/function_name.hh"
#include "db/functions/aggregate_function.hh"

namespace cql3 {
namespace functions {

class user_aggregate : public db::functions::aggregate_function, public data_dictionary::keyspace_element {
public:
    user_aggregate(function_name fname, bytes_opt initcond, ::shared_ptr<scalar_function> sfunc, ::shared_ptr<scalar_function> reducefunc, ::shared_ptr<scalar_function> finalfunc);
    bool has_finalfunc() const;

    virtual sstring keypace_name() const override { return name().keyspace; }
    virtual sstring element_name() const override { return name().name; }
    virtual sstring element_type() const override { return "aggregate"; }
    virtual std::ostream& describe(std::ostream& os) const override;

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
