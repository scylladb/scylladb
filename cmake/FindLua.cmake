#
# Copyright 2025-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Custom FindLua module that uses pkg-config, matching configure.py's
# approach.  CMake's built-in FindLua resolves to the versioned library
# (e.g. liblua-5.4.so) instead of the unversioned symlink (liblua.so),
# causing a name mismatch between the two build systems.

find_package(PkgConfig REQUIRED)

# configure.py: lua53 on Debian-like, lua on others
pkg_search_module(PC_lua QUIET lua53 lua)

find_library(Lua_LIBRARY
  NAMES lua lua5.3 lua53
  HINTS
    ${PC_lua_LIBDIR}
    ${PC_lua_LIBRARY_DIRS})

find_path(Lua_INCLUDE_DIR
  NAMES lua.h
  HINTS
    ${PC_lua_INCLUDEDIR}
    ${PC_lua_INCLUDE_DIRS})

mark_as_advanced(
  Lua_LIBRARY
  Lua_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(Lua
  REQUIRED_VARS
    Lua_LIBRARY
    Lua_INCLUDE_DIR
  VERSION_VAR PC_lua_VERSION)

if(Lua_FOUND)
  set(LUA_LIBRARIES ${Lua_LIBRARY})
  set(LUA_INCLUDE_DIR ${Lua_INCLUDE_DIR})
endif()

