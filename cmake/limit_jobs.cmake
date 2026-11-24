# Link jobs share one memory budget. A ninja job takes one pool slot whatever it
# costs, so the ThinLTO link runs in a pool of its own, one at a time, and its
# memory is set aside first; every other link shares what is left.
# Peak memory measured with and without full debug info, with headroom: the
# ThinLTO link of scylla peaked at 35-39GiB with 32 back-end threads; every
# other link (tests, non-LTO scylla) stayed under 6.3GiB.
set(Scylla_RAM_PER_LINK_JOB 8192 CACHE STRING
  "Memory budget for each link job without LTO (in MiB)")
set(Scylla_RAM_PER_LTO_LINK_JOB 45056 CACHE STRING
  "Memory budget for the ThinLTO link job (in MiB)")

if(NOT DEFINED Scylla_PARALLEL_LINK_JOBS)
  cmake_host_system_information(
    RESULT _total_mem_mb
    QUERY AVAILABLE_PHYSICAL_MEMORY)
  if(Scylla_ENABLE_LTO)
    math(EXPR _total_mem_mb "${_total_mem_mb} - ${Scylla_RAM_PER_LTO_LINK_JOB}")
  endif()
  math(EXPR _link_pool_depth "${_total_mem_mb} / ${Scylla_RAM_PER_LINK_JOB}")
  if(_link_pool_depth LESS 1)
    set(_link_pool_depth 1)
  endif()
  set(Scylla_PARALLEL_LINK_JOBS "${_link_pool_depth}" CACHE STRING
    "Maximum number of concurrent link jobs without LTO")
endif()

set_property(
  GLOBAL
  APPEND
  PROPERTY JOB_POOLS
    link_pool=${Scylla_PARALLEL_LINK_JOBS}
    lto_link_pool=1
    submodule_pool=1)
set(CMAKE_JOB_POOL_LINK link_pool)
