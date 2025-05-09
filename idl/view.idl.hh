/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

namespace db {
namespace view {
class update_backlog {
    size_t get_current_size();
    size_t get_max_size();
};
}
}
