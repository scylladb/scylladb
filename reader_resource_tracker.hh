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

/*
 * Copyright (C) 2017 ScyllaDB
 */

#pragma once

#include <core/file.hh>
#include <core/semaphore.hh>

class reader_resource_tracker {
    seastar::semaphore* _sem = nullptr;
public:
    reader_resource_tracker() = default;
    explicit reader_resource_tracker(seastar::semaphore* sem)
        : _sem(sem) {
    }

    bool operator==(const reader_resource_tracker& other) const {
        return _sem == other._sem;
    }

    file track(file f) const;

    semaphore* get_semaphore() const {
        return _sem;
    }
};

inline reader_resource_tracker no_resource_tracking() {
    return reader_resource_tracker(nullptr);
}
