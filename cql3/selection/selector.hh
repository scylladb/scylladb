/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "cql3/expr/expression.hh"

namespace cql3 {

namespace selection {


// An entry in the SELECT clause.
struct prepared_selector {
    expr::expression expr;
    ::shared_ptr<column_identifier> alias;
};

bool processes_selection(const prepared_selector&);


}

}
