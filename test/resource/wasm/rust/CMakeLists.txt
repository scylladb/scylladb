find_program(CARGO cargo
  REQUIRED)


function(compile_rust_to_wasm input)
  cmake_parse_arguments(parsed_args "" "WASM;OUT_DIR" "" ${ARGN})
  get_filename_component(basename ${input} NAME_WE)
  set(input "${CMAKE_CURRENT_SOURCE_DIR}/${input}")
  set(output_dir "${parsed_args_OUT_DIR}")
  set(output "${output_dir}/${basename}.wasm")
  set(target "wasm32-wasi")
  set(mode "debug")
  set(package "examples")
  add_custom_command(
    OUTPUT ${output}
    COMMAND
      ${CARGO} build
      --target=${target}
      --example=${basename}
      --locked
      --manifest-path=Cargo.toml
      --target-dir=${CMAKE_CURRENT_BINARY_DIR}
    COMMAND "wasm-opt"
      "${CMAKE_CURRENT_BINARY_DIR}/${target}/${profile}/${mode}/${package}/${basename}.wasm"
      "-Oz"
      "-o" "${output}"
    COMMAND "wasm-strip" "${output}"
    DEPENDS ${input} Cargo.lock
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building WASM ${output}"
    VERBATIM)
  set(${parsed_args_WASM} ${output} PARENT_SCOPE)
endfunction(compile_rust_to_wasm)

set(rust_srcs
  return_input.rs
  test_complex_null_values.rs
  test_functions_with_frozen_types.rs
  test_short_ints.rs
  test_types_with_and_without_nulls.rs)

foreach(rust_src ${rust_srcs})
  compile_rust_to_wasm(${rust_src}
    OUT_DIR "${CMAKE_BINARY_DIR}/wasm"
    WASM wasm)
  wasm2wat(${wasm}
    OUT_DIR "${CMAKE_BINARY_DIR}/wasm"
    WAT wat)
  list(APPEND wasms ${wat})
endforeach()

add_custom_target(wasm_rust
  DEPENDS ${wasms})
add_dependencies(wasm
  wasm_rust)
