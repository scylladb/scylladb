// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#include "aggregate_function.hh"

namespace db::functions {

aggregate_function::aggregate_function(stateless_aggregate_function agg, bool reducible_variant)
        : _agg(std::move(agg))
        , _reducible(!reducible_variant ? make_reducible_variant(_agg) : nullptr) {
}

const stateless_aggregate_function&
aggregate_function::get_aggregate() const {
    return _agg;
}

shared_ptr<aggregate_function>
aggregate_function::make_reducible_variant(stateless_aggregate_function agg) {
    if (!agg.state_reduction_function) {
        return nullptr;
    }
    auto new_agg = agg;
    new_agg.state_to_result_function = nullptr;
    new_agg.result_type = new_agg.aggregation_function->return_type();
    return make_shared<aggregate_function>(new_agg, true);
}

bool
aggregate_function::is_reducible() const {
    return bool(_agg.state_reduction_function);
}

::shared_ptr<aggregate_function>
aggregate_function::reducible_aggregate_function() {
    return _reducible;
}

const function_name&
aggregate_function::name() const {
    return _agg.name;
}

const std::vector<data_type>&
aggregate_function::arg_types() const {
    return _agg.argument_types;
}

const data_type&
aggregate_function::return_type() const {
    return _agg.result_type;
}

bool
aggregate_function::is_pure() const {
    return _agg.aggregation_function->is_pure()
        && (!_agg.state_to_result_function || _agg.state_to_result_function->is_pure())
        && (!_agg.state_reduction_function || _agg.state_reduction_function->is_pure());
}

bool
aggregate_function::is_native() const {
    return _agg.aggregation_function->is_native()
        && (!_agg.state_to_result_function || _agg.state_to_result_function->is_native())
        && (!_agg.state_reduction_function || _agg.state_reduction_function->is_native());
}

bool
aggregate_function::requires_thread() const {
    return _agg.aggregation_function->requires_thread()
        || (_agg.state_to_result_function && _agg.state_to_result_function->requires_thread())
        || (_agg.state_reduction_function && _agg.state_reduction_function->requires_thread());
}

bool
aggregate_function::is_aggregate() const {
    return true;
}

void
aggregate_function::print(std::ostream& os) const {
    fmt::print(os, "{}", name());
}

sstring
aggregate_function::column_name(const std::vector<sstring>& column_names) const {
    if (_agg.column_name_override) {
        return *_agg.column_name_override;
    }
    return format("{}({})", _agg.name, fmt::join(column_names, ", "));
}

}
