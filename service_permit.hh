/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

class service_permit {
    seastar::lw_shared_ptr<seastar::semaphore_units<>> _permit;
    service_permit(seastar::semaphore_units<>&& u) : _permit(seastar::make_lw_shared<seastar::semaphore_units<>>(std::move(u))) {}
    friend service_permit make_service_permit(seastar::semaphore_units<>&& permit);
    friend service_permit empty_service_permit();
public:
    size_t count() const { return _permit ? _permit->count() : 0; };
};

inline service_permit make_service_permit(seastar::semaphore_units<>&& permit) {
    return service_permit(std::move(permit));
}

inline service_permit empty_service_permit() {
    return make_service_permit(seastar::semaphore_units<>());
}
