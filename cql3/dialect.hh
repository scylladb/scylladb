// Copyright (C) 2024-present ScyllaDB
// SPDX-License-Identifier: AGPL-3.0-or-later

#pragma once

namespace cql3 {

struct dialect {
    bool duplicate_bind_variable_names_refer_to_same_variable = true;  // if :a is found twice in a query, the two references are to the same variable
};

}
