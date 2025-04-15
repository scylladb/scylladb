/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "locator/tablets.hh"
#include "schema/schema_fwd.hh"
namespace tasks {

struct virtual_task_hint {
    // Contains hints for all virtual tasks types.
    std::optional<table_id> table_id;
    std::optional<locator::tablet_task_type> task_type;
    std::optional<locator::tablet_id> tablet_id;

    locator::tablet_task_type get_task_type() const;
    locator::tablet_id get_tablet_id() const;
    ::table_id get_table_id() const;
};

}
