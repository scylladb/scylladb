/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include "utils/phased_barrier.hh"

class service_permit {
    struct data {
        seastar::semaphore_units<> units;
        utils::phased_barrier::operation op;
    };
    seastar::lw_shared_ptr<data> _permit;
    service_permit(seastar::semaphore_units<>&& u, utils::phased_barrier::operation op) : _permit(seastar::make_lw_shared<data>(std::move(u), std::move(op))) {}
    friend service_permit make_service_permit(seastar::semaphore_units<>&& permit, utils::phased_barrier::operation op);
    friend service_permit empty_service_permit();
public:
    size_t count() const { return _permit ? _permit->units.count() : 0; };
    operator bool() const noexcept {
        return bool(_permit);
    }
};

inline service_permit make_service_permit(seastar::semaphore_units<>&& permit, utils::phased_barrier::operation op = {}) {
    return service_permit(std::move(permit), std::move(op));
}

inline service_permit empty_service_permit() {
    return make_service_permit(seastar::semaphore_units<>(), utils::phased_barrier::operation());
}
