/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <ostream>
#include <functional>
#include "utils/UUID.hh"

namespace raft {
namespace internal {

template<typename Tag>
class tagged_uint64 {
    uint64_t _val;
public:
    tagged_uint64() : _val(0) {}
    explicit tagged_uint64(uint64_t v) : _val(v) {}
    tagged_uint64(const tagged_uint64&) = default;
    tagged_uint64(tagged_uint64&&) = default;
    tagged_uint64& operator=(const tagged_uint64&) = default;
    auto operator<=>(const tagged_uint64&) const = default;
    explicit operator bool() const { return _val != 0; }

    uint64_t get_value() const {
        return _val;
    }
    operator uint64_t() const {
        return get_value();
    }
    tagged_uint64& operator++() { // pre increment
        ++_val;
        return *this;
    }
    tagged_uint64 operator++(int) { // post increment
        uint64_t v = _val++;
        return tagged_uint64(v);
    }
    tagged_uint64& operator--() { // pre decrement
        --_val;
        return *this;
    }
    tagged_uint64 operator--(int) { // post decrement
        uint64_t v = _val--;
        return tagged_uint64(v);
    }
    tagged_uint64 operator+(const tagged_uint64& o) const {
        return tagged_uint64(_val + o._val);
    }
    tagged_uint64 operator-(const tagged_uint64& o) const {
        return tagged_uint64(_val - o._val);
    }
    friend std::ostream& operator<<(std::ostream& os, const tagged_uint64<Tag>& u) {
        os << u._val;
        return os;
    }
};

template<typename Tag>
struct tagged_id {
    utils::UUID id;
    bool operator==(const tagged_id& o) const {
        return id == o.id;
    }
    explicit operator bool() const {
        // The default constructor sets the id to nil, which is
        // guaranteed to not match any valid id.
        return id != utils::UUID();
    }
};

template<typename Tag>
std::ostream& operator<<(std::ostream& os, const tagged_id<Tag>& id) {
    os << id.id;
    return os;
}

} // end of namespace internal
} // end of namespace raft

namespace std {

template<typename Tag>
struct hash<raft::internal::tagged_id<Tag>> {
    size_t operator()(const raft::internal::tagged_id<Tag>& id) const {
        return hash<utils::UUID>()(id.id);
    }
};

} // end of namespace std

