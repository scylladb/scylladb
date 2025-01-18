set(Scylla_RAM_PER_LINK_JOB "4096" CACHE STRING
  "Maximum amount of memory used by each link job (in MiB)")
set(Scylla_PARALLEL_LINK_JOBS "" CACHE STRING
  "Maximum number of concurrent link jobs")

if(NOT Scylla_PARALLEL_LINK_JOBS)
  if(Scylla_ENABLE_LTO)
    message(STATUS "Linker performs ThinLTO optimization using all available CPU threads by default, but limiting parallel link jobs to 2 to prevent excessive memory usage")
    set(Scylla_PARALLEL_LINK_JOBS "2")
  else()
    cmake_host_system_information(
      RESULT _total_mem_mb
      QUERY AVAILABLE_PHYSICAL_MEMORY)
    math(EXPR _link_pool_depth "${_total_mem_mb} / ${Scylla_RAM_PER_LINK_JOB}")
    if(_link_pool_depth EQUAL 0)
      set(_link_pool_depth 1)
    endif()
    set(Scylla_RAM_PER_LINK_JOBS "${_link_pool_depth}")
  endif()
endif()

set_property(
  GLOBAL
  APPEND
  PROPERTY JOB_POOLS
    link_pool=${Scylla_PARALLEL_LINK_JOBS}
    submodule_pool=1)
set(CMAKE_JOB_POOL_LINK link_pool)
