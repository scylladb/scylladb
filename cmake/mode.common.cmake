set(disabled_warnings
  c++11-narrowing
  mismatched-tags
  overloaded-virtual
  unsupported-friend)
include(CheckCXXCompilerFlag)
foreach(warning ${disabled_warnings})
  check_cxx_compiler_flag("-Wno-${warning}" _warning_supported_${warning})
  if(_warning_supported_${warning})
    list(APPEND _supported_warnings ${warning})
  endif()
endforeach()
list(TRANSFORM _supported_warnings PREPEND "-Wno-")
string(JOIN " " CMAKE_CXX_FLAGS "-Wall" "-Werror" ${_supported_warnings})

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

math(EXPR _stack_usage_threshold_in_bytes "${stack_usage_threshold_in_KB} * 1024")
set(_stack_usage_threshold_flag "-Wstack-usage=${_stack_usage_threshold_in_bytes}")
check_cxx_compiler_flag(${_stack_usage_threshold_flag} _stack_usage_flag_supported)
if(_stack_usage_flag_supported)
  string(APPEND CMAKE_CXX_FLAGS " ${_stack_usage_threshold_flag}")
endif()
