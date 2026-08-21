/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include <seastar/core/on_internal_error.hh>

#include "cql3/prepare_context.hh"
#include "cql3/column_identifier.hh"
#include "cql3/column_specification.hh"
#include "cql3/functions/function.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {

static logging::logger prepare_context_logger("prepare_context");

size_t prepare_context::bound_variables_size() const {
    return _variable_names.size();
}

const std::vector<lw_shared_ptr<column_specification>>& prepare_context::get_variable_specifications() const & {
    return _variable_specs;
}

std::vector<lw_shared_ptr<column_specification>> prepare_context::get_variable_specifications() && {
    return std::move(_variable_specs);
}

std::vector<uint16_t> prepare_context::get_partition_key_bind_indexes(const schema& schema) const {
    auto count = schema.partition_key_columns().size();
    std::vector<uint16_t> partition_key_positions(count, uint16_t(0));
    std::vector<bool> set(count, false);

    for (auto&& [bind_index, target_spec] : _targets) {
        const auto* cdef = target_spec ? schema.get_column_definition(target_spec->name->name()) : nullptr;
        if (cdef && cdef->is_partition_key()) {
            partition_key_positions[cdef->position()] = bind_index;
            set[cdef->position()] = true;
        }
    }
    for (bool b : set) {
        if (!b) {
            return {};
        }
    }
    return partition_key_positions;
}

void prepare_context::add_variable_specification(int32_t bind_index, lw_shared_ptr<column_specification> spec) {
    auto name = _variable_names[bind_index];
    if (_variable_specs[bind_index]) {
        // If the same variable is used in multiple places, check that the types are compatible
        if (&spec->type->without_reversed() != &_variable_specs[bind_index]->type->without_reversed()) {
            throw exceptions::invalid_request_exception(
                    fmt::format("variable :{} has type {} which doesn't match {}",
                            *name, _variable_specs[bind_index]->type->as_cql3_type(), spec->name));
        }
    }
    _targets.emplace_back(bind_index, spec);
    // Use the user name, if there is one
    if (name) {
        spec = make_lw_shared<column_specification>(spec->ks_name, spec->cf_name, name, spec->type);
    }
    _variable_specs[bind_index] = spec;
}

void prepare_context::set_bound_variables(const std::vector<shared_ptr<column_identifier>>& bind_variable_names, dialect d) {
    _variable_names = bind_variable_names;
    _variable_specs.clear();
    _targets.clear();
    _dialect = d;

    const size_t bn_size = bind_variable_names.size();
    _variable_specs.resize(bn_size);
    _targets.resize(bn_size);
}

const dialect& prepare_context::get_dialect() const {
    if (!_dialect) {
        on_internal_error(prepare_context_logger, "the dialect of the prepared statement was never set");
    }
    return *_dialect;
}

void prepare_context::add_pk_function_call(expr::expression& e) {
    // The slot index travels between shards as a uint8_t, see
    // cql3::computed_function_values.
    constexpr auto fn_limit = std::numeric_limits<uint8_t>::max();
    if (_pk_function_calls.size() == fn_limit) {
        throw exceptions::invalid_request_exception(
            format("Too many function calls within one statement. Max supported number is {}", fn_limit));
    }

    auto& fn = expr::as<expr::function_call>(e);
    auto type = std::get<shared_ptr<db::functions::function>>(fn.func)->return_type();
    size_t index = _pk_function_calls.size();
    _pk_function_calls.push_back(e);
    e = expr::temporary{.index = index, .type = std::move(type), .replaced_expr = std::move(fn)};
}


}
