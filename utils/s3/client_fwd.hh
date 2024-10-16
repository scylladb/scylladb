/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <cstddef>

namespace s3 {
class client;

struct upload_progress {
    size_t total;
    size_t uploaded;
};
}
