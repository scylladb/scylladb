/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef TRANSFER_HH_
#define TRANSFER_HH_

// Helper functions for copying or moving multiple objects in an exception
// safe manner, then destroying the sources.
//
// To transfer, call transfer_pass1(allocator, &from, &to) on all object pairs,
// (this copies the object from @from to @to).  If no exceptions are encountered,
// call transfer_pass2(allocator, &from, &to).  This destroys the object at the
// origin.  If exceptions were encountered, simply destroy all copied objects.
//
// As an optimization, if the objects are moveable without throwing (noexcept)
// transfer_pass1() simply moves the objects and destroys the source, and
// transfer_pass2() does nothing.

#include <type_traits>

template <typename T, typename Alloc>
inline
void
transfer_pass1(Alloc& a, T* from, T* to,
        typename std::enable_if<std::is_nothrow_move_constructible<T>::value>::type* = nullptr) {
    a.construct(to, std::move(*from));
    a.destroy(from);
}

template <typename T, typename Alloc>
inline
void
transfer_pass2(Alloc& a, T* from, T* to,
        typename std::enable_if<std::is_nothrow_move_constructible<T>::value>::type* = nullptr) {
}

template <typename T, typename Alloc>
inline
void
transfer_pass1(Alloc& a, T* from, T* to,
        typename std::enable_if<!std::is_nothrow_move_constructible<T>::value>::type* = nullptr) {
    a.construct(to, *from);
}

template <typename T, typename Alloc>
inline
void
transfer_pass2(Alloc& a, T* from, T* to,
        typename std::enable_if<!std::is_nothrow_move_constructible<T>::value>::type* = nullptr) {
    a.destroy(from);
}

#endif /* TRANSFER_HH_ */
