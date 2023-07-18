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

class abort_requested_exception {
};

struct exception_variant {
    std::variant<replica::unknown_exception,
            replica::no_exception,
            replica::rate_limit_exception,
            replica::abort_requested_exception
    > reason;
};

}
