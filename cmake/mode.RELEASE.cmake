set(Seastar_OptimizationLevel_RELEASE "3")
set(CMAKE_CXX_FLAGS_RELEASE
  "-ffunction-sections -fdata-sections"
  CACHE
  INTERNAL
  "")
string(APPEND CMAKE_CXX_FLAGS_RELEASE
  " -O${Seastar_OptimizationLevel_RELEASE}")

set(Seastar_DEFINITIONS_DEBUG
  SCYLLA_BUILD_MODE=release)

set(CMAKE_STATIC_LINKER_FLAGS_RELEASE
  "-Wl,--gc-sections")
