/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/assert.hh"
#include <boost/intrusive/parent_from_member.hpp>
#include <assert.h>

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
        SCYLLA_ASSERT(!other._ref);
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
