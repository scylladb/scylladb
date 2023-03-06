# add the whole archive, otherwise linker would drop the unreferenced symbols.
# we cannot mark variables with `__attribute__(used)`, as it only applies to
# functions. another option is to `-Wl,--undefined=<symbol>` to the compiler,
# but we would be pass a mangled symbol name, that'd be convoluted without
# actually compiling a sample program.
function(add_whole_archive name library)
  add_library(${name} INTERFACE)
  if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.24)
    target_link_libraries(${name} INTERFACE
      "$<LINK_LIBRARY:WHOLE_ARCHIVE,${library}>")
  else()
    add_dependencies(${name} ${library})
    target_include_directories(${name} INTERFACE
      ${CMAKE_SOURCE_DIR})
    target_link_options(auth INTERFACE
      "$<$<CXX_COMPILER_ID:Clang>:SHELL:LINKER:-force_load $<TARGET_LINKER_FILE:${library}>>"
      "$<$<CXX_COMPILER_ID:GNU>:SHELL:LINKER:--whole-archive $<TARGET_LINKER_FILE:${library}> LINKER:--no-whole-archive>")
  endif()
endfunction()
