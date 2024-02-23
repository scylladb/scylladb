
/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "managed_bytes.hh"

bytes_opt
to_bytes_opt(const managed_bytes_opt& mbo) {
    if (!mbo) {
        return std::nullopt;
    }
    return mbo->with_linearized([] (bytes_view bv) {
        return bytes_opt(bv);
    });
}

managed_bytes_opt to_managed_bytes_opt(const bytes_opt& bo) {
    if (!bo) {
        return std::nullopt;
    }
    return managed_bytes(*bo);
}

sstring to_hex(const managed_bytes& b) {
    return seastar::format("{}", managed_bytes_view(b));
}

sstring to_hex(const managed_bytes_opt& b) {
    return !b ? "null" : to_hex(*b);
}
