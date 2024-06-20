/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace db {
namespace view {
class update_backlog {
    size_t get_current_bytes();
    size_t get_max_bytes();
};
}
}
