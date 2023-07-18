/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace replica {

struct unknown_exception {};

struct no_exception {};

class rate_limit_exception {
};

class stale_topology_exception {
    int64_t caller_version();
    int64_t callee_fence_version();
};

class abort_requested_exception {
};

struct exception_variant {
    std::variant<replica::unknown_exception,
            replica::no_exception,
            replica::rate_limit_exception,
            replica::stale_topology_exception,
            replica::abort_requested_exception
    > reason;
};

}
