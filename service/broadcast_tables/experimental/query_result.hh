/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <variant>

#include "bytes.hh"

namespace service::broadcast_tables {

// Represents result of single cell query.
struct query_result_select {
    bytes_opt value;
};

// Represents result of conditional update query.
struct query_result_conditional_update {
    bool is_applied;
    bytes_opt previous_value;
};

struct query_result_none {};

using query_result = std::variant<query_result_select, query_result_conditional_update, query_result_none>;

} // namespace service::broadcast_tables