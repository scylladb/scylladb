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

#include <optional>

#include "timestamp.hh"

class mutation;

namespace seastar {

template <typename PtrType>
class foreign_ptr;

template <typename T>
class lw_shared_ptr;

} // namespace seastar

namespace query {

class result;
class partition_slice;

} // namespace query

namespace service {

// An instance of this class is passed as an argument to storage_proxy::cas().
// The apply() method, which must be defined by the implementation. It can
// either return a mutation that will be used as a value for paxos 'propose'
// stage or it can return an empty option in which case an empty mutation will
// be used
class cas_request {
public:
    virtual ~cas_request() = default;
    // it is safe to dereference and use the qr foreign pointer, the result was
    // created by a foreign shard but no longer used by it.
    virtual std::optional<mutation> apply(seastar::foreign_ptr<seastar::lw_shared_ptr<query::result>> qr,
            const query::partition_slice& slice, api::timestamp_type ts) = 0;
};

} // namespace service