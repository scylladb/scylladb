/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include <cstddef>

namespace s3 {
class client;

struct upload_progress {
    size_t total = 0;
    size_t uploaded = 0;
    upload_progress operator+(const upload_progress& other) const { return {total + other.total, uploaded + other.uploaded}; }
};
}
