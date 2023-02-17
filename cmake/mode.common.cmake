set(disabled_warnings
  c++11-narrowing
  mismatched-tags
  missing-braces
  overloaded-virtual
  parentheses-equality
  unsupported-friend)
include(CheckCXXCompilerFlag)
foreach(warning disabled_warnings)
  check_cxx_compiler_flag("-Wno-${warning}" _warning_supported)
  if(_warning_supported)
    list(APPEND _supported_warnings ${warning})
  endif()
endforeach()
list(TRANSFORM disabled_warnings PREPEND "-Wno-")
string(JOIN " " CMAKE_CXX_FLAGS "-Wall" "-Werror" ${disabled_warnings})

function(default_target_arch arch)
  set(x86_instruction_sets i386 i686 x86_64)
  if(CMAKE_SYSTEM_PROCESSOR IN_LIST x86_instruction_sets)
    set(${arch} "westmere" PARENT_SCOPE)
  elseif(CMAKE_SYSTEM_PROCESSOR EQUAL "aarch64")
    set(${arch} "armv8-a+crc+crypto" PARENT_SCOPE)
  else()
    set(${arch} "" PARENT_SCOPE)
  endif()
endfunction()

default_target_arch(target_arch)
if(target_arch)
    string(APPEND CMAKE_CXX_FLAGS " -march=${target_arch}")
endif()
