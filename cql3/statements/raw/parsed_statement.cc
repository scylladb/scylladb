/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "parsed_statement.hh"

#include "cql3/statements/prepared_statement.hh"
#include "cql3/column_specification.hh"

namespace cql3 {

namespace statements {

namespace raw {

parsed_statement::~parsed_statement()
{ }

prepare_context& parsed_statement::get_prepare_context() {
    return _prepare_ctx;
}

const prepare_context& parsed_statement::get_prepare_context() const {
    return _prepare_ctx;
}

// Used by the parser and preparable statement
void parsed_statement::set_bound_variables(const std::vector<::shared_ptr<column_identifier>>& bound_names) {
    _prepare_ctx.set_bound_variables(bound_names);
}

}

prepared_statement::prepared_statement(
        ::shared_ptr<cql_statement> statement_, std::vector<lw_shared_ptr<column_specification>> bound_names_,
        std::vector<uint16_t> partition_key_bind_indices, std::vector<sstring> warnings)
    : statement(std::move(statement_))
    , bound_names(std::move(bound_names_))
    , partition_key_bind_indices(std::move(partition_key_bind_indices))
    , warnings(std::move(warnings))
{ }

prepared_statement::prepared_statement(
        ::shared_ptr<cql_statement> statement_, const prepare_context& ctx,
        const std::vector<uint16_t>& partition_key_bind_indices, std::vector<sstring> warnings)
    : prepared_statement(statement_, ctx.get_variable_specifications(), partition_key_bind_indices, std::move(warnings))
{ }

prepared_statement::prepared_statement(::shared_ptr<cql_statement> statement_, prepare_context&& ctx, std::vector<uint16_t>&& partition_key_bind_indices)
    : prepared_statement(statement_, std::move(ctx).get_variable_specifications(), std::move(partition_key_bind_indices))
{ }

prepared_statement::prepared_statement(::shared_ptr<cql_statement>&& statement_)
    : prepared_statement(statement_, std::vector<lw_shared_ptr<column_specification>>(), std::vector<uint16_t>())
{ }

}

}
