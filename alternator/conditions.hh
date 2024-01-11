/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "expressions_types.hh"

namespace alternator {

enum class comparison_operator_type {
    EQ, NE, LE, LT, GE, GT, IN, BETWEEN, CONTAINS, NOT_CONTAINS, IS_NULL, NOT_NULL, BEGINS_WITH
};

comparison_operator_type get_comparison_operator(const rjson::value& comparison_operator);

enum class conditional_operator_type {
    AND, OR, MISSING
};
conditional_operator_type get_conditional_operator(const rjson::value& req);

bool verify_expected(const rjson::value& req, const rjson::value* previous_item);
bool verify_condition(const rjson::value& condition, bool require_all, const rjson::value* previous_item);

bool check_CONTAINS(const rjson::value* v1, const rjson::value& v2, bool v1_from_query, bool v2_from_query);
bool check_BEGINS_WITH(const rjson::value* v1, const rjson::value& v2, bool v1_from_query, bool v2_from_query);

bool verify_condition_expression(
        const parsed::condition_expression& condition_expression,
        const rjson::value* previous_item);

}
