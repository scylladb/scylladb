/*
 */

/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "cql3/expr/expression.hh"

namespace cql3 {

namespace restrictions {

/**
 * Result of relation::to_restriction().  TODO: remove this class and rewrite to_restriction to return
 * expression.
 */
class restriction {
public:
    // Init to false for now, to easily detect errors.  This whole class is going away.
    cql3::expr::expression expression = expr::constant::make_bool(false);
    virtual ~restriction() {}
};

}

}
