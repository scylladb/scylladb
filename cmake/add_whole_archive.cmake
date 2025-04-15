# add the whole archive, otherwise linker would drop the unreferenced symbols.
# we cannot mark variables with `__attribute__(used)`, as it only applies to
# functions. another option is to `-Wl,--undefined=<symbol>` to the compiler,
# but we would be pass a mangled symbol name, that'd be convoluted without
# actually compiling a sample program.
function(add_whole_archive name library)
  add_library(${name} INTERFACE)
  target_link_libraries(${name} INTERFACE
    "$<LINK_LIBRARY:WHOLE_ARCHIVE,${library}>")
endfunction()
