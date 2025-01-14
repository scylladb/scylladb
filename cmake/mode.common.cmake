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

# We want to strip the absolute build paths from the binary,
# so that logs and debuggers show e.g. ./main.cc,
# not /var/lib/jenkins/workdir/scylla/main.cc, or something.
#
# The base way to do that is -ffile-prefix-map=${CMAKE_SOURCE_DIR}/=
# But by itself, it results in *both* DW_AT_name and DW_AT_comp_dir being
# subject to the substitution.
# For example, if table::query() is located
# in /home/user/scylla/replica/table.cc,
# and the compiler working directory is /home/user/scylla/build,
# then after the ffile-prefix-map substitution it will
# have DW_AT_comp_dir equal to ./build
# and DW_AT_name equal to ./replica/table.cc
#
# If DW_AT_name is a relative path, gdb looks for the source files in $DW_AT_comp_dir/$DW_AT_name.
# This results in e.g. gdb looking for seastar::thread_context::main
# in ./build/./replica/table.cc
# instead of replica/table.cc as we would like.
# To unscrew this, we have to add a rule which will 
# convert the /absolute/path/to/build to `.`,
# which will result in gdb looking in ././replica/table.cc, which is fine.
#
# The build rule which converts `/absolute/path/to/build/` (note trailing slash)
# to `build/` exists just so that any DW_AT_name under build (e.g. in generated sources)
# is excluded from the first rule.
#
# Note that the order of these options is important.
# Each is strictly more specific than the previous one.
# If they were the other way around, only the most general rule would be used.
add_compile_options("-ffile-prefix-map=${CMAKE_SOURCE_DIR}/=")
add_compile_options("-ffile-prefix-map=${CMAKE_BINARY_DIR}=.")
cmake_path(GET CMAKE_BINARY_DIR FILENAME build_dir_name)
add_compile_options("-ffile-prefix-map=${CMAKE_BINARY_DIR}/=${build_dir_name}")

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

set(pgo_opts "")
# Clang supports three instrumenttation methods for profile generation:
# - -fprofile-instr-generate: this happens in the frontend (AST phase)
# - -fprofile-generate: this happens in the middle end. this instruments the LLVM IR
#   generated by the front-end. and is called the regular PGO.
# - -fcs-profile-generate: "cs" is short for Context Sensitive. this instrumentation
#   method is called CSPGO in comparison with the regular PGO above.
# We use IR and CSIR to represent the last two instrumentation methods in the option
# of Scylla_BUILD_INSTRUMENTED. the frontend instrumentation is not supported, because
# the IR-based instrumentation is superier than the frontend-based instrumentation when
# profiling executable for optimization purposes.
set(Scylla_BUILD_INSTRUMENTED OFF CACHE STRING
    "Build ScyllaDB with PGO instrumentation. May be specified as IR, CSIR")
if(Scylla_BUILD_INSTRUMENTED)
  file(TO_NATIVE_PATH "${CMAKE_BINARY_DIR}/${Scylla_BUILD_INSTRUMENTED}" Scylla_PROFILE_DATA_DIR)
  if(Scylla_BUILD_INSTRUMENTED STREQUAL "IR")
    # instrument code at IR level, also known as the regular PGO
    string(APPEND pgo_opts " -fprofile-generate=\"${Scylla_PROFILE_DATA_DIR}\"")
  elseif(Scylla_BUILD_INSTRUMENTED STREQUAL "CSIR")
    # instrument code with Context Sensitive IR, also known as CSPGO.
    string(APPEND pgo_opts " -fcs-profile-generate=\"${Scylla_PROFILE_DATA_DIR}\"")
  else()
    message(FATAL_ERROR "Unknown Scylla_BUILD_INSTRUMENTED: ${}")
  endif()
endif()

set(Scylla_PROFDATA_FILE "" CACHE FILEPATH
  "Path to the profiling data file to use when compiling.")
set(Scylla_PROFDATA_COMPRESSED_FILE "" CACHE FILEPATH
  "Path to the compressed profiling data file to use when compiling")
if(Scylla_PROFDATA_FILE AND Scylla_PROFDATA_COMPRESSED_FILE)
  message(FATAL_ERROR
    "Both Scylla_PROFDATA_FILE and Scylla_PROFDATA_COMPRESSED_FILE are specified!")
endif()

function(extract_compressed_file)
  find_program(XZCAT xzcat
    REQUIRED)

  cmake_parse_arguments(parsed_args "" "INPUT;OUTPUT" "" ${ARGN})
  set(input ${parsed_args_INPUT})

  get_filename_component(ext "${input}" LAST_EXT)
  get_filename_component(stem "${input}" NAME_WLE)
  set(output "${CMAKE_BINARY_DIR}/${stem}")
  if(ext STREQUAL ".xz")
    execute_process(
      COMMAND ${XZCAT} "${input}"
      OUTPUT_FILE "${output}"
      WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}")
  else()
    message(FATAL_ERROR "Unknown compression format: ${ext}")
  endif()
  set(${parsed_args_OUTPUT} ${output} PARENT_SCOPE)
endfunction(extract_compressed_file)

if(Scylla_PROFDATA_FILE)
  if(NOT EXISTS "${Scylla_PROFDATA_FILE}")
    message(FATAL_ERROR
      "Specified Scylla_PROFDATA_FILE (${Scylla_PROFDATA_FILE}) does not exist")
  endif()
  set(profdata_file "${Scylla_PROFDATA_FILE}")
elseif(Scylla_PROFDATA_COMPRESSED_FILE)
  # read the header to see if the file is fetched by LFS upon checkout
  file(READ "${Scylla_PROFDATA_COMPRESSED_FILE}" file_header LIMIT 7)
  if(file_header MATCHES "version")
    message(FATAL_ERROR "Please install git-lfs for using profdata stored in Git LFS")
  endif()
  extract_compressed_file(
    INPUT "${Scylla_PROFDATA_COMPRESSED_FILE}"
    OUTPUT "profdata_file")
endif()

if(profdata_file)
  if(Scylla_BUILD_INSTRUMENTED STREQUAL "IR")
    # -fprofile-use is not allowed with -fprofile-generate
    message(WARNING "Only CSIR supports using and generating profdata at the same time.")
    unset(pgo_opts)
  endif()
  # When building with PGO, -Wbackend-plugin generates a warning for every
  # function which changed its control flow graph since the profile was
  # taken.
  # We allow stale profiles, so these warnings are just noise to us.
  # Let's silence them.
  string(APPEND CMAKE_CXX_FLAGS " -Wno-backend-plugin")
  string(APPEND CMAKE_CXX_FLAGS " -fprofile-use=\"${profdata_file}\"")
endif()

if(pgo_opts)
  string(APPEND CMAKE_CXX_FLAGS "${pgo_opts}")
  string(APPEND CMAKE_EXE_LINKER_FLAGS "${pgo_opts}")
  string(APPEND CMAKE_SHARED_LINKER_FLAGS "${pgo_opts}")
endif()

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

if(Scylla_ENABLE_LTO)
  include(CheckIPOSupported)
  block()
    set(CMAKE_EXE_LINKER_FLAGS "${linker_flag}")
    set(CMAKE_TRY_COMPILE_PLATFORM_VARIABLES CMAKE_EXE_LINKER_FLAGS)
    check_ipo_supported(RESULT ipo_supported OUTPUT error)
    if(NOT ipo_supported)
      message(FATAL_ERROR "LTO is not supported: ${error}")
    endif()
  endblock()
endif()
