// Copyright (C) 2023-present ScyllaDB
// SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)

#pragma once

#include <optional>
#include "expression.hh"

namespace cql3 {

class query_options;

}

namespace cql3::expr {

// Some expression users can behave differently if the expression is a bind variable
// and if that bind variable is unset. unset_bind_variable_guard encapsulates the two
// conditions.
class unset_bind_variable_guard {
    // Disengaged if the operand is not exactly a single bind variable.
    std::optional<bind_variable> _var;
public:
    explicit unset_bind_variable_guard(const expr::expression& operand);
    explicit unset_bind_variable_guard(std::nullopt_t) {}
    explicit unset_bind_variable_guard(const std::optional<expr::expression>& operand);
    bool is_unset(const query_options& qo) const;
};

}
