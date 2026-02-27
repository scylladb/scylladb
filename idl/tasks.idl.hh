/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/uuid.idl.hh"

verb [[with_client_info]] tasks_get_children (tasks::get_children_request req) -> tasks::get_children_response;
