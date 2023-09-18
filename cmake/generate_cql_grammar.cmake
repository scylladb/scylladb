find_program (ANTLR3 antlr3
  REQUIRED)

# Parse antlr3 grammar files and generate C++ sources
function(generate_cql_grammar)
  cmake_parse_arguments(parsed_args "" "GRAMMAR;OUTPUT_DIR;SOURCES" "" ${ARGN})
  if(IS_ABSOLUTE ${parsed_args_GRAMMAR})
    set(grammar ${parsed_args_GRAMMAR})
  else()
    set(grammar ${CMAKE_CURRENT_SOURCE_DIR}/${parsed_args_GRAMMAR})
  endif()
  if(parsed_args_OUTPUT_DIR)
    set(gen_dir "${parsed_args_OUTPUT_DIR}")
  else()
    set(gen_dir "${CMAKE_CURRENT_BINARY_DIR}")
  endif()
  get_filename_component(name "${grammar}" NAME)
  set(grammar_processed ${gen_dir}/${name})

  get_filename_component(stem "${grammar}" NAME_WE)
  foreach (postfix "Lexer.hpp" "Lexer.cpp" "Parser.hpp" "Parser.cpp")
    set(filename "${stem}${postfix}")
    list(APPEND outputs "${filename}")
    list(APPEND remove_timestamps COMMAND sed -i -e "/^.*On : .*$/d" ${filename})
  endforeach()

  add_custom_command(
    DEPENDS ${grammar}
    OUTPUT ${outputs}
    # Remove #ifdef'ed code from the grammar source code
    COMMAND sed -e "/^#if 0/,/^#endif/d" ${grammar} > ${grammar_processed}
    COMMAND ${ANTLR3} "${grammar_processed}"
    ${remove_timestamps}
    # We replace many local `ExceptionBaseType* ex` variables with a single function-scope one.
    # Because we add such a variable to every function, and because `ExceptionBaseType` is not a global
    # name, we also add a global typedef to avoid compilation errors.
    COMMAND sed -i -e "s/^\\( *\\)\\(ImplTraits::CommonTokenType\\* [a-zA-Z0-9_]* = NULL;\\)$/\\1const \\2/"
      -e "1i using ExceptionBaseType = int;"
      -e "s/^{{/{{ ExceptionBaseType\* ex = nullptr;/"
      -e "s/ExceptionBaseType\* ex = new/ex = new/"
      -e "s/exceptions::syntax_exception e/exceptions::syntax_exception\\& e/"
      ${gen_dir}/${stem}Parser.cpp
    COMMENT "Generating sources from ${grammar}"
    VERBATIM)

  set(${parsed_args_SOURCES} ${outputs} PARENT_SCOPE)
endfunction(generate_cql_grammar)
