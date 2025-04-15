/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

namespace db {
struct replay_position {
    uint64_t id;
    uint32_t pos;
};
}
