/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "seastarx.hh"

#include <optional>
#include <vector>
#include <stddef.h>
#include "cql3/expr/expression.hh"
#include "cql3/dialect.hh"

class schema;

namespace cql3 {

class column_identifier;
class column_specification;
namespace functions { class function_call; }

/**
 * Metadata class currently holding bind variables specifications and
 * `function_call` AST nodes inside a query partition key restrictions.
 * Populated and maintained at "prepare" step of query execution.
 */
class prepare_context final {
private:
    // Keeps names of all the bind variables. For bind variables without a name ('?'), the name is nullptr.
    // Maps bind_index -> name.
    std::vector<shared_ptr<column_identifier>> _variable_names;

    // Keeps column_specification for every bind_index. column_specification describes the name and type of this variable.
    std::vector<lw_shared_ptr<column_specification>> _variable_specs;

    // For every expression like (<target> = <bind variable>), there's a pair of (bind_index, target column_specification) in _targets.
    // Collecting all equalities of bind variables allows to determine which of the variables set the value of partition key columns.
    // The driver needs this information in order to compute the partition token and send the request to the right node.
    std::vector<std::pair<std::size_t, lw_shared_ptr<column_specification>>> _targets;

    // The non-pure `function_call`s that participate in partition key ranges
    // computation, in the order they were found. Each was replaced in the
    // statement's expressions by an `expr::temporary` holding its index here,
    // which is also its slot in the temporaries vector query_options carries.
    std::vector<expr::expression> _pk_function_calls;

    // The flag denoting whether the context is currently in partition key
    // processing mode (inside query restrictions AST nodes). If set to true,
    // then every non-pure `function_call` instance will be recorded in the
    // context and replaced by a temporary, whose value is computed once per
    // request and survives a bounce to another shard.
    bool _processing_pk_restrictions = false;

    // The dialect the statement is parsed and prepared under. Captured when the
    // request starts being processed, so that everything the prepare step decides
    // is decided under a single value, even if the node configuration changes
    // while the statement is being prepared. There is no default: preparing under
    // a dialect nobody chose is worse than not preparing at all.
    std::optional<dialect> _dialect;

public:

    prepare_context() = default;

    size_t bound_variables_size() const;

    const std::vector<lw_shared_ptr<column_specification>>& get_variable_specifications() const &;

    std::vector<lw_shared_ptr<column_specification>> get_variable_specifications() &&;

    std::vector<uint16_t> get_partition_key_bind_indexes(const schema& schema) const;

    void add_variable_specification(int32_t bind_index, lw_shared_ptr<column_specification> spec);

    // Hands over what the parser knows and the prepare step needs. The dialect is
    // part of it, so that a statement built rather than parsed cannot reach the
    // prepare step without having said which dialect it is prepared under.
    void set_bound_variables(const std::vector<shared_ptr<column_identifier>>& bind_variable_names, dialect d);

    const dialect& get_dialect() const;

    // Record a new function call, which evaluates a partition key constraint,
    // and replace it in `e` with the temporary that stands in for it. The
    // caller has to make sure `e` holds a prepared `expr::function_call`.
    void add_pk_function_call(cql3::expr::expression& e);

    // The function calls recorded by add_pk_function_call(), indexed by the
    // slot of the temporary that replaced them. Whoever evaluates expressions
    // holding those temporaries is responsible for filling their slots.
    const std::vector<expr::expression>& pk_function_calls() const {
        return _pk_function_calls;
    }

    // Inform the context object that it has started or ended processing the
    // partition key part of statement restrictions.
    void set_processing_pk_restrictions(bool flag) noexcept {
        _processing_pk_restrictions = flag;
    }

    bool is_processing_pk_restrictions() const noexcept {
        return _processing_pk_restrictions;
    }
};

}
