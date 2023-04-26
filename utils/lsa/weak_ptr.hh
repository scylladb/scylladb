/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/entangled.hh"
#include <seastar/core/shared_ptr.hh>

namespace lsa {

///
/// This module introduces a mechanism for stable weak pointers to LSA-managed objects
/// which are not invalidated by LSA and can be obtained without locking the region.
/// This simplifies code which needs to implement cursors into LSA-based data structures.
///
/// The pointers are non-owning (weak).
/// When the object is not referenced by any weak_ptr, the memory overhead is one pointer
/// per weakly_referencable<> object.
///
/// Example:
///
///     struct X : public lsa::weakly_referencable<X> {
///         int value;
///     };
///
///     lsa::weak_ptr<X> x_ptr = with_allocator(region(), [] {
///           X* x = current_allocator().construct<X>();
///           return x->weak_from_this();
///     });
///
///     std::cout << x_ptr->value;
///
/// Below you can find a diagram which presents relationships between objects:
///
///                   L S A          |    standard allocator
///                                  |
///    -------------------------     |
///   | T                       |    |
///   |     -----------------   |    |     ----------------------
///   |    | weakly_referen  |  |    |    | sharing_guard<T>     |      -------------
///   |    | cable<T>        |  |    |    |                      |<----| weak_ptr<T> |
///   |    |   -----------   |  |    |    |  -----------         |      -------------
///   |    |  | entangled |~~~~~~~~~~~~~~~~~| entangled |        |
///   |    |   -----------   |  |    |    |  -----------         |      -------------
///   |    |                 |  |    |    |                      |<----| weak_ptr<T> |
///   |     -----------------   |    |    |                      |      -------------
///   |                         |    |    +----------------------+
///   +-------------------------+    |
///                                  |
///
///   The sharing_guard objects is needed to track the location of the LSA-managed object via
///   "entangled" instance. It does so on behalf of all weak_ptr:s. The guard lives
///   only as long as there is any weak_ptr alive.
///


template<typename T>
class weakly_referencable;

// Managed by the standard allocator.
// References to it are stable.
template<typename T>
class sharing_guard : public seastar::enable_lw_shared_from_this<sharing_guard<T>> {
    entangled _ref; // Paired with weakly_referencable::_ref
    friend class weakly_referencable<T>;
public:
    explicit sharing_guard(entangled&& r) : _ref(std::move(r)) {}
    // Returns nullptr iff !engaged().
    T* get();
    bool engaged() const { return bool(_ref); }
};

// A weak pointer to an LSA-managed object.
//
// Pointers to T obtained through the weak_ptr instance are valid only as
// long as the allocation strategy of T does not invalidate references.
// After references to T are invalidated, this instance can be dereferenced again
// to obtain a valid reference to T.
template<typename T>
class weak_ptr {
    seastar::lw_shared_ptr<sharing_guard<T>> _ptr;
public:
    explicit weak_ptr(seastar::lw_shared_ptr<sharing_guard<T>> p) : _ptr(std::move(p)) {}
    weak_ptr() = default;

    weak_ptr& operator=(std::nullptr_t) noexcept {
        _ptr = nullptr;
        return *this;
    }

    T* get() { return _ptr ? _ptr->get() : nullptr; }
    const T* get() const { return _ptr ? _ptr->get() : nullptr; }
    T* operator->() { return get(); }
    const T* operator->() const { return get(); }
    T& operator*() { return *(operator->()); }
    const T& operator*() const { return *(operator->()); }

    explicit operator bool() const { return _ptr && _ptr->engaged(); }

    bool operator==(const weak_ptr& o) const noexcept = default;

    // Returns true iff there are no other weak_ptr:s to this object.
    bool unique() const {
        return _ptr.use_count() == 1;
    }
};

// Managed by LSA allocator.
template<typename T>
class weakly_referencable {
    // Paired with sharing_guard::_ref
    // Engaged when and only when this object has at least one active weak_ptr.
    entangled _ref;
    friend class sharing_guard<T>;
public:
    // Obtains a weak_ptr referencing the instance of T which inherits from this object.
    // Does not have to be called with the LSA region of T locked.
    // May invalidate references to this object because it allocates memory.
    // The returned weak_ptr<> is always valid.
    weak_ptr<T> weak_from_this() {
        if (_ref) {
            sharing_guard<T>* s = _ref.get(&sharing_guard<T>::_ref);
            return weak_ptr<T>(s->shared_from_this());
        } else {
            // We need to link _ref to a stable entangled on stack before calling
            // make_lw_shared() because the latter may allocate memory and trigger
            // reclamation and thus invalidate "this" and so this->_ref.
            // References on stack are not invalidated.
            auto r = entangled::make_paired_with(_ref);
            return weak_ptr<T>(seastar::make_lw_shared<sharing_guard<T>>(std::move(r)));
        }
    }

    explicit operator bool() const { return bool(_ref); }
    bool is_referenced() const { return bool(_ref); }
};

template<typename T>
T* sharing_guard<T>::get() {
    return static_cast<T*>(_ref.get(&weakly_referencable<T>::_ref));
}

}
