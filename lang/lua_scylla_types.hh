/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <lua.hpp>
#include "types/types.hh"

namespace lua {

template <typename T>
static T* aligned_user_data(lua_State* l) {
    constexpr size_t alignment = alignof(T);
    // We know lua_newuserdata aligns allocations to 8, so we need a
    // padding of at most alignment - 8 to find a sufficiently aligned
    // address.
    static_assert(alignment>= 8);
    constexpr size_t pad = alignment - 8;
    char* p = reinterpret_cast<char*>(lua_newuserdata(l, sizeof(T) + pad));
    return reinterpret_cast<T*>(align_up(p, alignment));
}

void register_metatables(lua_State* l);

void push_sstring(lua_State* l, const sstring& v);

void push_data_value(lua_State* l, const data_value& value);
data_value pop_data_value(lua_State* l, const data_type& type);

} // namespace lua
