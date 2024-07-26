/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#pragma once

#include "cql3/expr/expression.hh"

namespace cql3::selection {

expr::expression make_count_rows_function_expression();
bool selectable_processes_selection(const expr::expression& raw_selectable);

}
