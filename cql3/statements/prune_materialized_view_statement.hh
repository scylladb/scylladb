/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

/* Copyright 2022-present ScyllaDB */

#pragma once

#include "cql3/statements/select_statement.hh"
#include "cql3/selection/selection.hh"

namespace cql3 {

namespace statements {

class prune_materialized_view_statement : public primary_key_select_statement {
public:
    prune_materialized_view_statement(schema_ptr schema,
                     uint32_t bound_terms,
                     lw_shared_ptr<const parameters> parameters,
                     ::shared_ptr<selection::selection> selection,
                     ::shared_ptr<restrictions::statement_restrictions> restrictions,
                     ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                     bool is_reversed,
                     ordering_comparator_type ordering_comparator,
                     std::optional<expr::expression> limit,
                     std::optional<expr::expression> per_partition_limit,
                     cql_stats &stats,
                     std::unique_ptr<cql3::attributes> attrs) : primary_key_select_statement(schema, bound_terms, parameters, selection, restrictions, group_by_cell_indices, is_reversed, ordering_comparator, limit, per_partition_limit, stats, std::move(attrs)) {}

private:
    virtual future<::shared_ptr<cql_transport::messages::result_message>> do_execute(query_processor& qp,
            service::query_state& state, const query_options& options) const override;
};

}

}
