/*
 * Copyright (C) 2015 ScyllaDB
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

#include "utils/allocation_strategy.hh"

template<typename T>
class managed;

//
// Similar to std::unique_ptr<>, but for LSA-allocated objects. Remains
// valid across deferring points. See make_managed().
//
// std::unique_ptr<> can't be used with LSA-allocated objects because
// it assumes that the object doesn't move after being allocated. This
// is not true for LSA, which moves objects during compaction.
//
// Also works for objects allocated using standard allocators, though
// there the extra space overhead of a pointer is not justified.
// It still make sense to use it in places which are meant to work
// with either kind of allocator.
//
template<typename T>
struct managed_ref {
    managed<T>* _ptr;

    managed_ref() : _ptr(nullptr) {}

    managed_ref(const managed_ref&) = delete;

    managed_ref(managed_ref&& other) noexcept
        : _ptr(other._ptr)
    {
        other._ptr = nullptr;
        if (_ptr) {
            _ptr->_backref = &_ptr;
        }
    }

    ~managed_ref() {
        if (_ptr) {
            current_allocator().destroy(_ptr);
        }
    }

    managed_ref& operator=(managed_ref&& o) {
        this->~managed_ref();
        new (this) managed_ref(std::move(o));
        return *this;
    }

    T& operator*() {
        return _ptr->_value;
    }

    const T& operator*() const {
        return _ptr->_value;
    }

    T* operator->() {
        return &_ptr->_value;
    }

    const T* operator->() const {
        return &_ptr->_value;
    }

    explicit operator bool() const {
        return _ptr != nullptr;
    }
};

template<typename T>
class managed {
    managed<T>** _backref;
    T _value;

    template<typename T_>
    friend struct managed_ref;
public:
    static_assert(std::is_nothrow_move_constructible<T>::value, "Throwing move constructor not supported");

    managed(managed<T>** backref, T&& v) noexcept
        : _backref(backref)
        , _value(std::move(v))
    {
        *_backref = this;
    }

    managed(managed&& other) noexcept
        : _backref(other._backref)
        , _value(std::move(other._value))
    {
        *_backref = this;
    }
};

//
// Allocates T using given AllocationStrategy and returns a managed_ref owning the
// allocated object.
//
template<typename T, typename... Args>
managed_ref<T>
make_managed(Args&&... args) {
    managed_ref<T> ref;
    current_allocator().construct<managed<T>>(&ref._ptr, T(std::forward<Args>(args)...));
    return ref;
}
