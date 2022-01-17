/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace db {
struct replay_position {
    uint64_t id;
    uint32_t pos;
};
}
