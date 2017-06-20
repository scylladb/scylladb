##
## For best results, first compile the project using the Ninja build-system.
##

cmake_minimum_required(VERSION 3.5)
project(scylla)

if (NOT DEFINED ENV{CLION_IDE})
    message(FATAL_ERROR "This CMakeLists.txt file is only valid for use in CLion")
endif()

# Default value. A more accurate list is populated through `pkg-config` below if `seastar.pc` is available.
set(SEASTAR_INCLUDE_DIRS "seastar")

find_package(PkgConfig REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14 -DHAVE_HWLOC -DHAVE_DPDK")

set(ENV{PKG_CONFIG_PATH} "${CMAKE_SOURCE_DIR}/seastar/build/release:$ENV{PKG_CONFIG_PATH}")
pkg_check_modules(SEASTAR seastar)

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

target_include_directories(scylla PRIVATE
        .
        ${SEASTAR_INCLUDE_DIRS}
        build/release/gen)
