find_package(cryptopp REQUIRED)
find_package(rapidxml REQUIRED)

add_library(utils STATIC)
target_sources(utils
  PRIVATE
    UUID_gen.cc
    arch/powerpc/crc32-vpmsum/crc32_wrapper.cc
    arch/powerpc/crc32-vpmsum/crc32.S
    array-search.cc
    ascii.cc
    base64.cc
    big_decimal.cc
    bloom_calculations.cc
    bloom_filter.cc
    buffer_input_stream.cc
    build_id.cc
    config_file.cc
    directories.cc
    disk-error-handler.cc
    dynamic_bitset.cc
    error_injection.cc
    exceptions.cc
    file_lock.cc
    gz/crc_combine.cc
    gz/crc_combine_table.cc
    hashers.cc
    histogram_metrics_helper.cc
    human_readable.cc
    i_filter.cc
    large_bitset.cc
    like_matcher.cc
    limiting_data_source.cc
    lister.cc
    logalloc.cc
    managed_bytes.cc
    multiprecision_int.cc
    murmur_hash.cc
    on_internal_error.cc
    pretty_printers.cc
    rate_limiter.cc
    rjson.cc
    runtime.cc
    to_string.cc
    updateable_value.cc
    utf8.cc
    uuid.cc
    aws_sigv4.cc
    s3/client.cc)
target_include_directories(utils
  PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${RAPIDJSON_INCLUDE_DIRS})
target_link_libraries(utils
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    Boost::regex
    cryptopp::cryptopp
    rapidxml::rapidxml)

check_headers(check-headers utils
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
