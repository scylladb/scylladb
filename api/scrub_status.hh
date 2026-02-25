/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

namespace api {

enum class scrub_status {
    successful = 0,
    aborted,
    unable_to_cancel,   // Not used in Scylla, included to ensure compatibility with nodetool api.
    validation_errors,
};

} // namespace api
