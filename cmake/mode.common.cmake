set(disabled_warnings
  c++11-narrowing
  deprecated-copy
  mismatched-tags
  missing-field-initializers
  overloaded-virtual
  unsupported-friend
  enum-constexpr-conversion
  unused-parameter)
include(CheckCXXCompilerFlag)
foreach(warning ${disabled_warnings})
  check_cxx_compiler_flag("-Wno-${warning}" _warning_supported_${warning})
  if(_warning_supported_${warning})
    list(APPEND _supported_warnings ${warning})
  endif()
endforeach()
list(TRANSFORM _supported_warnings PREPEND "-Wno-")
add_compile_options(
  "-Wall"
  "-Werror"
  "-Wextra"
  "-Wno-error=deprecated-declarations"
  "-Wimplicit-fallthrough"
  ${_supported_warnings})

function(default_target_arch arch)
  set(x86_instruction_sets i386 i686 x86_64)
  if(CMAKE_SYSTEM_PROCESSOR IN_LIST x86_instruction_sets)
    set(${arch} "westmere" PARENT_SCOPE)
  elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    # we always use intrinsics like vmull.p64 for speeding up crc32 calculations
    # on the aarch64 architectures, and they require the crypto extension, so
    # we have to add "+crypto" in the architecture flags passed to -march. the
    # same applies to crc32 instructions, which need the ARMv8-A CRC32 extension
    # please note, Seastar also sets -march when compiled with DPDK enabled.
    set(${arch} "armv8-a+crc+crypto" PARENT_SCOPE)
  else()
    set(${arch} "" PARENT_SCOPE)
  endif()
endfunction()

function(pad_at_begin output fill str length)
  # pad the given `${str} with `${fill}`, right aligned. with the syntax of
  # fmtlib:
  #   fmt::print("{:#>{}}", str, length)
  # where `#` is the `${fill}` char
  string(LENGTH "${str}" str_len)
  math(EXPR padding_len "${length} - ${str_len}")
  if(padding_len GREATER 0)
    string(REPEAT ${fill} ${padding_len} padding)
  endif()
  set(${output} "${padding}${str}" PARENT_SCOPE)
endfunction()

# The relocatable package includes its own dynamic linker. We don't
# know the path it will be installed to, so for now use a very long
# path so that patchelf doesn't need to edit the program headers.  The
# kernel imposes a limit of 4096 bytes including the null. The other
# constraint is that the build-id has to be in the first page, so we
# can't use all 4096 bytes for the dynamic linker.
# In here we just guess that 2000 extra / should be enough to cover
# any path we get installed to but not so large that the build-id is
# pushed to the second page.
# At the end of the build we check that the build-id is indeed in the
# first page. At install time we check that patchelf doesn't modify
# the program headers.
function(get_padded_dynamic_linker_option output length)
  set(dynamic_linker_option "-dynamic-linker")
  # capture the drive-generated command line first
  execute_process(
    COMMAND ${CMAKE_C_COMPILER} "-###" /dev/null -o t
    ERROR_VARIABLE driver_command_line
    ERROR_STRIP_TRAILING_WHITESPACE)
  # extract the argument for the "-dynamic-linker" option
  if(driver_command_line MATCHES ".*\"?${dynamic_linker_option}\"? \"?([^ \"]*)\"? .*")
    set(dynamic_linker ${CMAKE_MATCH_1})
  else()
    message(FATAL_ERROR "Unable to find ${dynamic_linker_option} in driver-generated command: "
      "${driver_command_line}")
  endif()
  # prefixing a path with "/"s does not actually change it means
  pad_at_begin(padded_dynamic_linker "/" "${dynamic_linker}" ${length})
  set(${output} "${dynamic_linker_option}=${padded_dynamic_linker}" PARENT_SCOPE)
endfunction()

add_compile_options("-ffile-prefix-map=${CMAKE_BINARY_DIR}=.")

default_target_arch(target_arch)
if(target_arch)
  add_compile_options("-march=${target_arch}")
endif()

add_compile_options("SHELL:-Xclang -fexperimental-assignment-tracking=disabled")

function(maybe_limit_stack_usage_in_KB stack_usage_threshold_in_KB config)
  math(EXPR _stack_usage_threshold_in_bytes "${stack_usage_threshold_in_KB} * 1024")
  set(_stack_usage_threshold_flag "-Wstack-usage=${_stack_usage_threshold_in_bytes}")
  check_cxx_compiler_flag(${_stack_usage_threshold_flag} _stack_usage_flag_supported)
  if(_stack_usage_flag_supported)
    add_compile_options($<$<CONFIG:${config}>:${_stack_usage_threshold_flag}>)
  endif()
endfunction()

macro(update_cxx_flags flags)
  cmake_parse_arguments (
    parsed_args
    "WITH_DEBUG_INFO"
    "OPTIMIZATION_LEVEL"
    ""
    ${ARGN})
  if(NOT DEFINED parsed_args_OPTIMIZATION_LEVEL)
    message(FATAL_ERROR "OPTIMIZATION_LEVEL is missing")
  endif()
  string(APPEND ${flags}
    " -O${parsed_args_OPTIMIZATION_LEVEL}")
  if(parsed_args_WITH_DEBUG_INFO)
    string(APPEND ${flags} " -g -gz")
  endif()
endmacro()

# Force SHA1 build-id generation
add_link_options("LINKER:--build-id=sha1")
include(CheckLinkerFlag)
set(Scylla_USE_LINKER
    ""
    CACHE
    STRING
    "Use specified linker instead of the default one")
if(Scylla_USE_LINKER)
    set(linkers "${Scylla_USE_LINKER}")
else()
    set(linkers "ld.lld" "gold")
endif()

foreach(linker ${linkers})
  if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(linker_flag "--ld-path=${linker}")
  else()
    set(linker_flag "-fuse-ld=${linker}")
  endif()

    check_linker_flag(CXX ${linker_flag} "CXX_LINKER_HAVE_${linker}")
    if(CXX_LINKER_HAVE_${linker})
        add_link_options("${linker_flag}")
        break()
    elseif(Scylla_USE_LINKER)
        message(FATAL_ERROR "${Scylla_USE_LINKER} is not supported.")
    endif()
endforeach()

if(DEFINED ENV{NIX_CC})
  get_padded_dynamic_linker_option(dynamic_linker_option 0)
else()
  # gdb has a SO_NAME_MAX_PATH_SIZE of 512, so limit the path size to
  # that. The 512 includes the null at the end, hence the 511 below.
  get_padded_dynamic_linker_option(dynamic_linker_option 511)
endif()
add_link_options("${dynamic_linker_option}")
