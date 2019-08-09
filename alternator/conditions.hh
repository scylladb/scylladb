/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

/*
 * This file contains definitions and functions related to placing conditions
 * on Alternator queries (equivalent of CQL's restrictions).
 *
 * With conditions, it's possible to add criteria to selection requests (Scan, Query)
 * and use them for narrowing down the result set, by means of filtering or indexing.
 *
 * Ref: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Condition.html
 */

#pragma once

#include "cql3/restrictions/statement_restrictions.hh"
#include "serialization.hh"

namespace alternator {

enum class comparison_operator_type {
    EQ, NE, LE, LT, GE, GT, IN, BETWEEN, CONTAINS, IS_NULL, NOT_NULL, BEGINS_WITH
};

comparison_operator_type get_comparison_operator(const rjson::value& comparison_operator);

::shared_ptr<cql3::restrictions::statement_restrictions> get_filtering_restrictions(schema_ptr schema, const column_definition& attrs_col, const rjson::value& query_filter);

}
