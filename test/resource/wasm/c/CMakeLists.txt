find_program(CLANG clang
  REQUIRED)

function(compile_c_to_wasm input)
  cmake_parse_arguments(parsed_args "" "WASM;OUT_DIR" "" ${ARGN})
  get_filename_component(basename ${input} NAME_WE)
  set(input "${CMAKE_CURRENT_SOURCE_DIR}/${input}")
  set(output_dir "${parsed_args_OUT_DIR}")
  set(output "${output_dir}/${basename}.wasm")
  set(target "wasm32")
  add_custom_command(
    OUTPUT ${output}
    COMMAND ${CLANG} ${input}
      --target=wasm32
      --no-standard-libraries
      -Wl,--export-all -Wl,--no-entry
      -o ${output}
    DEPENDS ${input}
    COMMENT "Building WASM ${output}")
  set(${parsed_args_WASM} ${output} PARENT_SCOPE)
endfunction(compile_c_to_wasm)

set(c_srcs
  test_UDA_final.c
  test_UDA_scalar.c
  test_fib_called_on_null.c
  test_mem_grow.c
  test_pow.c
  test_word_double.c)

foreach(c_src ${c_srcs})
  compile_c_to_wasm(${c_src}
    OUT_DIR "${CMAKE_BINARY_DIR}/wasm"
    WASM wasm)
  wasm2wat(${wasm}
    OUT_DIR "${CMAKE_BINARY_DIR}/wasm"
    WAT wat)
  list(APPEND wasms ${wat})
endforeach()

add_custom_target(wasm_c
  DEPENDS ${wasms})
add_dependencies(wasm
  wasm_c)
