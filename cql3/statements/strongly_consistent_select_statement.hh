/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/expr/expression.hh"
#include "cql3/statements/select_statement.hh"

namespace cql3 {

namespace statements {

namespace broadcast_tables {

struct prepared_select {
    expr::expression key;
};

}

class strongly_consistent_select_statement : public select_statement {
    const broadcast_tables::prepared_select _query;

    broadcast_tables::prepared_select prepare_query() const;
public:
    strongly_consistent_select_statement(schema_ptr schema,
                     uint32_t bound_terms,
                     lw_shared_ptr<const parameters> parameters,
                     ::shared_ptr<selection::selection> selection,
                     ::shared_ptr<const restrictions::statement_restrictions> restrictions,
                     ::shared_ptr<std::vector<size_t>> group_by_cell_indices,
                     bool is_reversed,
                     ordering_comparator_type ordering_comparator,
                     std::optional<expr::expression> limit,
                     std::optional<expr::expression> per_partition_limit,
                     cql_stats &stats,
                     std::unique_ptr<cql3::attributes> attrs);

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
        execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;
};

}

}
