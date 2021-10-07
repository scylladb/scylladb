/*
 * Copyright (C) 2021-present ScyllaDB
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

#include "../../reader_concurrency_semaphore.hh"
#include "query_class_config.hh"

namespace tests {

// Must be used in a seastar thread.
class reader_concurrency_semaphore_wrapper {
    std::unique_ptr<::reader_concurrency_semaphore> _stored_semaphore;
    ::reader_concurrency_semaphore& _semaphore;

public:
    explicit reader_concurrency_semaphore_wrapper(reader_concurrency_semaphore& semaphore)
        : _semaphore(semaphore)
    { }
    explicit reader_concurrency_semaphore_wrapper(const char* name = nullptr)
        : _stored_semaphore(std::make_unique<::reader_concurrency_semaphore>(::reader_concurrency_semaphore::no_limits{}, name ? name : "test"))
        , _semaphore(*_stored_semaphore)
    { }
    ~reader_concurrency_semaphore_wrapper() {
        if (_stored_semaphore) {
            _stored_semaphore->stop().get();
        }
    }

    reader_concurrency_semaphore& semaphore() { return _semaphore; };
    reader_permit make_permit() { return _semaphore.make_tracking_only_permit(nullptr, "test", db::no_timeout); }
};

} // namespace tests
