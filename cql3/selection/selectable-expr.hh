/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#pragma once

#include "selectable.hh"
#include "cql3/expr/expression.hh"
#include "data_dictionary/data_dictionary.hh"

namespace cql3::selection {

expr::expression make_count_rows_function_expression();
shared_ptr<selectable> prepare_selectable(const schema& s, const expr::expression& raw_selectable, data_dictionary::database db, const sstring& keyspace);
bool selectable_processes_selection(const expr::expression& raw_selectable);

}
