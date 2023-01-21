/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/abstract_marker.hh"
#include "cql3/expr/expression.hh"
#include "utils/like_matcher.hh"

namespace cql3 {

    // Retrieve parameter marker values, if any, find the appropriate collection
    // element if the cell is a collection and an element access is used in the expression,
    // and evaluate the condition.
    bool column_condition_applies_to(const expr::expression& expr, const expr::evaluation_inputs& inputs);

    expr::expression column_condition_prepare(const expr::expression& expr, data_dictionary::database db, const sstring& keyspace, const schema& schema);

} // end of namespace cql3
