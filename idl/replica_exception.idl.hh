/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace replica {

class unknown_exception {
    seastar::sstring get_cause() [[ref]];
};

class timeout_exception {
};

class forward_exception {
};

class virtual_table_update_exception {
    seastar::sstring get_cause() [[ref]];
}

struct exception_variant {
    std::variant<std::monostate,
            replica::unknown_exception,
            replica::timeout_exception,
            replica::forward_exception,
            replica::virtual_table_update_exception
    > reason;
};

}
