##
## For best results, first compile the project using the Ninja build-system.
##

cmake_minimum_required(VERSION 3.7)
project(scylla)

if (NOT DEFINED FOR_IDE AND NOT DEFINED ENV{FOR_IDE} AND NOT DEFINED ENV{CLION_IDE})
    message(FATAL_ERROR "This CMakeLists.txt file is only valid for use in IDEs, please define FOR_IDE to acknowledge this.")
endif()

# Default value. A more accurate list is populated through `pkg-config` below if `seastar.pc` is available.
set(SEASTAR_INCLUDE_DIRS "seastar")

# These paths are always available, since they're included in the repository. Additional DPDK headers are placed while
# Seastar is built, and are captured in `SEASTAR_INCLUDE_DIRS` through parsing the Seastar pkg-config file (below).
set(SEASTAR_DPDK_INCLUDE_DIRS
        seastar/dpdk/lib/librte_eal/common/include
        seastar/dpdk/lib/librte_eal/common/include/generic
        seastar/dpdk/lib/librte_eal/common/include/x86
        seastar/dpdk/lib/librte_ether)

find_package(PkgConfig REQUIRED)

set(ENV{PKG_CONFIG_PATH} "${CMAKE_SOURCE_DIR}/seastar/build/release:$ENV{PKG_CONFIG_PATH}")
pkg_check_modules(SEASTAR seastar)

find_package(Boost COMPONENTS filesystem program_options system thread)

##
## Populate the names of all source and header files in the indicated paths in a designated variable.
##
## When RECURSIVE is specified, directories are traversed recursively.
##
## Use: scan_scylla_source_directories(VAR my_result_var [RECURSIVE] PATHS [path1 path2 ...])
##
function (scan_scylla_source_directories)
    set(options RECURSIVE)
    set(oneValueArgs VAR)
    set(multiValueArgs PATHS)
    cmake_parse_arguments(args "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")

    set(globs "")

    foreach (dir ${args_PATHS})
        list(APPEND globs "${dir}/*.cc" "${dir}/*.hh")
    endforeach()

    if (args_RECURSIVE)
        set(glob_kind GLOB_RECURSE)
    else()
        set(glob_kind GLOB)
    endif()

    file(${glob_kind} var
            ${globs})

    set(${args_VAR} ${var} PARENT_SCOPE)
endfunction()

## Although Seastar is an external project, it is common enough to explore the sources while doing
## Scylla development that we'll treat the Seastar sources as part of this project for easier navigation.
scan_scylla_source_directories(
        VAR SEASTAR_SOURCE_FILES
        RECURSIVE

        PATHS
          seastar/core
          seastar/http
          seastar/json
          seastar/net
          seastar/rpc
          seastar/tests
          seastar/util)

scan_scylla_source_directories(
        VAR SCYLLA_ROOT_SOURCE_FILES
        PATHS .)

scan_scylla_source_directories(
        VAR SCYLLA_SUB_SOURCE_FILES
        RECURSIVE

        PATHS
          api
          auth
          cql3
          db
          dht
          exceptions
          gms
          index
          io
          locator
          message
          repair
          service
          sstables
          streaming
          tests
          thrift
          tracing
          transport
          utils)

scan_scylla_source_directories(
        VAR SCYLLA_GEN_SOURCE_FILES
        RECURSIVE
        PATHS build/release/gen)

set(SCYLLA_SOURCE_FILES
        ${SCYLLA_ROOT_SOURCE_FILES}
        ${SCYLLA_GEN_SOURCE_FILES}
        ${SCYLLA_SUB_SOURCE_FILES})

add_executable(scylla
        ${SEASTAR_SOURCE_FILES}
        ${SCYLLA_SOURCE_FILES})

# Note that since CLion does not undestand GCC6 concepts, we always disable them (even if users configure otherwise).
# CLion seems to have trouble with `-U` (macro undefinition), so we do it this way instead.
list(REMOVE_ITEM SEASTAR_CFLAGS "-DHAVE_GCC6_CONCEPTS")

# If the Seastar pkg-config information is available, append to the default flags.
#
# For ease of browsing the source code, we always pretend that DPDK is enabled.
target_compile_options(scylla PUBLIC
        -std=gnu++1z
        -DHAVE_DPDK
        -DHAVE_HWLOC
        "${SEASTAR_CFLAGS}")

# The order matters here: prefer the "static" DPDK directories to any dynamic paths from pkg-config. Some files are only
# available dynamically, though.
target_include_directories(scylla PUBLIC
        .
        ${SEASTAR_DPDK_INCLUDE_DIRS}
        ${SEASTAR_INCLUDE_DIRS}
        ${Boost_INCLUDE_DIRS}
        xxhash
        libdeflate
        build/release/gen)
