/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "expression.hh"

namespace cql3 {
namespace expr {

// Given a restriction from the WHERE clause prepares it and performs some validation checks.
// It will also fill the prepare context automatically, there's no need to do that later.
binary_operator validate_and_prepare_new_restriction(const binary_operator& restriction,
                                                     data_dictionary::database db,
                                                     schema_ptr schema,
                                                     prepare_context& ctx);
} // namespace expr
} // namespace cql3
