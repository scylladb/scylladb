/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema_fwd.hh"
namespace tasks {

struct virtual_task_hint {
    // Contains hints for all virtual tasks types.
    std::optional<table_id> table_id;
};

}
