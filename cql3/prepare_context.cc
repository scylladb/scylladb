/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/prepare_context.hh"
#include "cql3/column_identifier.hh"
#include "cql3/column_specification.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {

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

void prepare_context::set_bound_variables(const std::vector<shared_ptr<column_identifier>>& bind_variable_names) {
    _variable_names = bind_variable_names;
    _variable_specs.clear();
    _targets.clear();

    const size_t bn_size = bind_variable_names.size();
    _variable_specs.resize(bn_size);
    _targets.resize(bn_size);
}

void prepare_context::clear_pk_function_calls_cache() {
    for (::shared_ptr<std::optional<uint8_t>>& cache_id : _pk_function_calls_cache_ids) {
        if (cache_id.get() != nullptr) {
            *cache_id = std::nullopt;
        }
    }
}

void prepare_context::add_pk_function_call(expr::function_call& fn) {
    constexpr auto fn_limit = std::numeric_limits<uint8_t>::max();
    if (_pk_function_calls_cache_ids.size() == fn_limit) {
        throw exceptions::invalid_request_exception(
            format("Too many function calls within one statement. Max supported number is {}", fn_limit));
    }

    fn.lwt_cache_id = ::make_shared<std::optional<uint8_t>>(_pk_function_calls_cache_ids.size());
    _pk_function_calls_cache_ids.emplace_back(fn.lwt_cache_id);
}


}
