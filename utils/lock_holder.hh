/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/rwlock.hh>
#include <seastar/core/shared_ptr.hh>

namespace utils {
    struct rwlock_holder {
        seastar::lw_shared_ptr<seastar::rwlock> lock;
        seastar::rwlock::holder holder;
    };
}

