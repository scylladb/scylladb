/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "seastar/core/sharded.hh"
#include "seastarx.hh"


namespace service {

struct fencing_token {
    int64_t topology_version;
};

inline bool operator<(const fencing_token& a, const fencing_token& b) {
    return a.topology_version < b.topology_version;
}

inline std::ostream& operator<<(std::ostream& os, const fencing_token& fencing_token) {
    return os << "fencing_token(" << fencing_token.topology_version << ")";
}

struct stale_fencing_token : public std::runtime_error {
    stale_fencing_token(const fencing_token& caller_token, const fencing_token& current_token);
};

class rpc_fence : public seastar::peering_sharded_service<rpc_fence> {
    fencing_token current_token;
public:
    explicit rpc_fence(fencing_token current_token);
    void check(const fencing_token& caller_token);
    future<> increment(fencing_token expected_token);
    inline fencing_token get_token() const noexcept {
        return current_token;
    }
};
} // end of namespace service
