/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <seastar/core/future.hh>

namespace db {
class system_keyspace;
}

namespace service {

struct topology;
class group0_guard;

seastar::future<bool> ongoing_rf_change(const topology& topology, db::system_keyspace& sys_ks, const group0_guard& guard, seastar::sstring ks);

}