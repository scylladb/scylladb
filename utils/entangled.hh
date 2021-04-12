/*
 * Copyright (C) 2020 ScyllaDB
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

#include <boost/intrusive/parent_from_member.hpp>

//  A movable pointer-like object paired with exactly one other object of the same type. 
//  The two objects which are paired with each other point at each other.
//  The relationship is symmetrical.
//
//  A pair of such objects can be used for implementing bi-directional traversal in data structures.
//
//  Moving this object automatically updates the other reference, so the references remain
//  consistent when the containing objects are managed by LSA.
//
//                get()
//          ------------------>
//   -----------             -----------
//  | entangled |~~~~~~~~~~~| entangled |
//   -----------             -----------
//          <------------------
//                get()
//
class entangled final {
    entangled* _ref = nullptr;
private:
    struct init_tag {};
    entangled(init_tag, entangled& other) {
        assert(!other._ref);
        _ref = &other;
        other._ref = this;
    }
public:
    // Creates a new object which is paired with a given "other".
    // The other is also paired with this object afterwards.
    // The other must not be paired before the call.
    static entangled make_paired_with(entangled& other) {
        return entangled(init_tag(), other);
    }

    // Creates an unpaired object.
    entangled() = default;
    entangled(const entangled&) = delete;

    entangled(entangled&& other) noexcept
        : _ref(other._ref)
    {
        if (_ref) {
            _ref->_ref = this;
        }
        other._ref = nullptr;
    }

    ~entangled() {
        if (_ref) {
            _ref->_ref = nullptr;
        }
    }

    entangled& operator=(entangled&& other) noexcept {
        if (_ref) {
            _ref->_ref = nullptr;
        }
        _ref = other._ref;
        if (_ref) {
            _ref->_ref = this;
        }
        other._ref = nullptr;
        return *this;
    }

    entangled* get() { return _ref; }
    const entangled* get() const { return _ref; }
    explicit operator bool() const { return _ref != nullptr; }

    template<typename T>
    T* get(entangled T::* paired_member) {
        if (!_ref) {
            return nullptr;
        }
        return boost::intrusive::get_parent_from_member(get(), paired_member);
    }

    template<typename T>
    const T* get(entangled T::* paired_member) const {
        if (!_ref) {
            return nullptr;
        }
        return boost::intrusive::get_parent_from_member(get(), paired_member);
    }
};
