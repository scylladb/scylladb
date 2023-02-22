// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)

#pragma once

#include "aggregate_function.hh"
#include "scalar_function.hh"
#include "function_name.hh"
#include <optional>

namespace db::functions {

struct stateless_aggregate_function final {
    function_name name;
    std::optional<sstring> column_name_override; // if unset, column name is synthesized from name and argument names

    data_type state_type;
    data_type result_type;
    std::vector<data_type> argument_types;

    bytes_opt initial_state;

    // aggregates another input
    // signature: (state_type, argument_types...) -> state_type
    shared_ptr<scalar_function> aggregation_function;

    // converts the state type to a result
    // signature: (state_type) -> result_type
    shared_ptr<scalar_function> state_to_result_function;

    // optional: reduces states computed in parallel
    // signature: (state_type, state_type) -> state_type
    shared_ptr<scalar_function> state_reduction_function;
};

class stateless_aggregate_function_adapter : public aggregate_function {
protected:
    stateless_aggregate_function _agg;
private:
    shared_ptr<aggregate_function> _reducible;
private:
    class aggregate_adapter;
    static shared_ptr<aggregate_function> make_reducible_variant(stateless_aggregate_function saf);
public:
    explicit stateless_aggregate_function_adapter(stateless_aggregate_function saf, bool reducible_variant = false);
    virtual std::unique_ptr<aggregate> new_aggregate() override;
    virtual bool is_reducible() const override;
    virtual ::shared_ptr<aggregate_function> reducible_aggregate_function() override;
    virtual const function_name& name() const override;
    virtual const std::vector<data_type>& arg_types() const override;
    virtual const data_type& return_type() const override;
    virtual bool is_pure() const override;
    virtual bool is_native() const override;
    virtual bool requires_thread() const override;
    virtual bool is_aggregate() const override;
    virtual void print(std::ostream& os) const override;
    virtual sstring column_name(const std::vector<sstring>& column_names) const override;
};

}
