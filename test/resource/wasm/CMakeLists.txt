find_program(WASM2WAT wasm2wat
  REQUIRED)

function(wasm2wat input)
  cmake_parse_arguments(parsed_args "" "WAT;OUT_DIR" "" ${ARGN})
  get_filename_component(basename ${input} NAME_WE)
  set(output_dir "${parsed_args_OUT_DIR}")
  set(output "${output_dir}/${basename}.wat")
  add_custom_command(
    OUTPUT ${output}
    COMMAND ${WASM2WAT} ${input} --output=${output}
    DEPENDS ${input}
    COMMENT "Generating WebAssembly Text ${output}")
  set(${parsed_args_WAT} ${output} PARENT_SCOPE)
endfunction()

add_custom_target(wasm ALL)
add_dependencies(tests wasm)
add_subdirectory(c)
add_subdirectory(rust)
