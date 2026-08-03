find_program(CARGO cargo
  REQUIRED)

# Set up RUSTC_WRAPPER for sccache support if configured
if(Scylla_RUSTC_WRAPPER)
  set(RUSTC_WRAPPER_ENV "RUSTC_WRAPPER=${Scylla_RUSTC_WRAPPER}")
else()
  set(RUSTC_WRAPPER_ENV "")
endif()

# `cargo build` runs as a single Ninja job, but internally spawns its own
# rustc jobs sized to all available cores, uncoordinated with Ninja's -j
# budget. On a clean build this stacks Ninja's parallel Clang jobs on top of
# Cargo's parallel rustc jobs at the same time, which can double real
# concurrency and exhaust memory. Cap Cargo to a quarter of the cores so it
# stays a minor contributor alongside the C++ build instead of an unbounded one.
include(ProcessorCount)
ProcessorCount(Scylla_RUST_NPROC)
if(Scylla_RUST_NPROC EQUAL 0)
  set(Scylla_RUST_NPROC 4)
endif()
math(EXPR Scylla_RUST_JOBS "(${Scylla_RUST_NPROC} + 3) / 4")

function(add_rust_library name)
  # used for profiles defined in Cargo.toml
  if(CMAKE_CONFIGURATION_TYPES)
    foreach(config ${CMAKE_CONFIGURATION_TYPES})
      string(APPEND build_mode
        "$<$<CONFIG:${config}>:${scylla_build_mode_${config}}>")
    endforeach()
  else()
    set(build_mode ${scylla_build_mode_${CMAKE_BUILD_TYPE}})
  endif()
  set(profile "rust-${build_mode}")
  set(target_dir ${CMAKE_CURRENT_BINARY_DIR})
  set(library ${target_dir}/lib${name}.a)
  # Match configure.py: --disable-wasmtime builds the rust workspace without
  # its default "wasmtime" cargo feature.
  if(Scylla_BUILD_WASMTIME)
    set(cargo_features "")
  else()
    set(cargo_features "--no-default-features")
  endif()
  add_custom_command(
    OUTPUT ${library}
    COMMAND ${CMAKE_COMMAND} -E env CARGO_BUILD_DEP_INFO_BASEDIR=. ${RUSTC_WRAPPER_ENV} ${CARGO} build --locked --jobs=${Scylla_RUST_JOBS} ${cargo_features} --target-dir=${target_dir} --profile=${profile}
    COMMAND ${CMAKE_COMMAND} -E copy ${target_dir}/${profile}/lib${name}.a ${library}
    DEPENDS Cargo.lock
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Rust library: ${library}")
  add_custom_target(rust-${name}
    DEPENDS ${target_dir}/lib${name}.a)
  add_library(Rust::${name} STATIC IMPORTED)
  add_dependencies(Rust::${name} rust-${name})
  set_target_properties(Rust::${name} PROPERTIES
    IMPORTED_LOCATION "${library}")
endfunction(add_rust_library)

find_program(CXXBRIDGE cxxbridge
  REQUIRED)
# Generate cxxbridge header and source
function(generate_cxxbridge name)
  cmake_parse_arguments(parsed_args
    ""
    "INPUT;OUTPUT_DIR;INCLUDE;SOURCES" "" ${ARGN})
  set(input ${CMAKE_CURRENT_SOURCE_DIR}/${parsed_args_INPUT})
  set(output_dir ${parsed_args_OUTPUT_DIR})
  set(header ${output_dir}/${name}.hh)
  set(source ${output_dir}/${name}.cc)
  set(outputs ${header} ${source})
  set(command ${CXXBRIDGE} ${input} --include ${parsed_args_INCLUDE})
  add_custom_command(
    DEPENDS ${input}
    OUTPUT ${outputs}
    COMMAND ${command} --header --output ${header}
    COMMAND ${command} --output ${source})

  set(${parsed_args_SOURCES} ${outputs} PARENT_SCOPE)
endfunction(generate_cxxbridge)


add_rust_library(rust_combined)

set(binding_gen_build_dir "${scylla_gen_build_dir}/rust")

set(cxx_header "${binding_gen_build_dir}/cxx.h")
add_custom_command(
  OUTPUT ${cxx_header}
  COMMAND ${CXXBRIDGE} --header --output ${cxx_header})
set_target_properties(Rust::rust_combined PROPERTIES
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}")

if(Scylla_BUILD_WASMTIME)
  generate_cxxbridge(wasmtime_bindings
    INPUT wasmtime_bindings/src/lib.rs
    INCLUDE rust/cxx.h
    OUTPUT_DIR "${binding_gen_build_dir}"
    SOURCES wasmtime_bindings_sources)

  add_library(wasmtime_bindings STATIC)
  target_sources(wasmtime_bindings
    PRIVATE
      ${cxx_header}
      ${wasmtime_bindings_sources})
  target_include_directories(wasmtime_bindings
    PUBLIC
      ${scylla_gen_build_dir})
  target_link_libraries(wasmtime_bindings
    INTERFACE Rust::rust_combined)
  if (Scylla_USE_PRECOMPILED_HEADER_USE)
    # The PCH from scylla-precompiled-header is compiled with Seastar's compile
    # flags, including sanitizer flags in Debug/Sanitize modes. Any target reusing
    # this PCH must have matching compile options, otherwise the compiler rejects
    # the PCH due to flag mismatch (e.g., -fsanitize=address).
    target_link_libraries(wasmtime_bindings PRIVATE Seastar::seastar)
    target_precompile_headers(wasmtime_bindings REUSE_FROM scylla-precompiled-header)
  endif()
else()
  # Stub keeping the many `target_link_libraries(... wasmtime_bindings)`
  # references valid; still carries the rust_combined archive (inc crate).
  add_library(wasmtime_bindings INTERFACE)
  target_link_libraries(wasmtime_bindings
    INTERFACE Rust::rust_combined)
endif()

# only for testing
generate_cxxbridge(inc
  INPUT inc/src/lib.rs
  INCLUDE rust/cxx.h
  OUTPUT_DIR "${binding_gen_build_dir}"
  SOURCES inc_sources)

add_library(inc STATIC)
target_sources(inc
  PRIVATE
    ${cxx_header}
    ${inc_sources})
target_include_directories(inc
  PUBLIC
    ${scylla_gen_build_dir})
target_link_libraries(inc
  INTERFACE Rust::rust_combined)
if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_link_libraries(inc PRIVATE Seastar::seastar)
  target_precompile_headers(inc REUSE_FROM scylla-precompiled-header)
endif()
