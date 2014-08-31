/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef VLA_HH_
#define VLA_HH_

#include <memory>
#include <new>
#include <assert.h>
#include <type_traits>

// Some C APIs have a structure with a variable length array at the end.
// This is a helper function to help allocate it.
//
// for a structure
//
//   struct xx { int a; float b[0]; };
//
// use
//
//   make_struct_with_vla(&xx::b, number_of_bs);
//
// to allocate it.
//
template <class S, typename E>
inline
std::unique_ptr<S>
make_struct_with_vla(E S::*last, size_t nr) {
    auto fake = reinterpret_cast<S*>(0);
    size_t offset = reinterpret_cast<uintptr_t>(&(fake->*last));
    size_t element_size = sizeof((fake->*last)[0]);
    assert(offset == sizeof(S));
    auto p = ::operator new(offset + element_size * nr);
    return std::unique_ptr<S>(new (p) S());
}



#endif /* VLA_HH_ */
