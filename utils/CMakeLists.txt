find_package(cryptopp REQUIRED)
find_package(rapidxml REQUIRED)
find_package(GnuTLS 3.3.26 REQUIRED)
find_package(Python3 COMPONENTS QUIET REQUIRED Interpreter)

# Generate aws_error_definitions_generated.{hh,cc} under the build tree from
# the source templates in utils/s3/, using the current AWS c2j service models
# for S3 and STS. The stitching of these generated files into the compile
# graph is done elsewhere.
set(aws_error_gen_script
  "${CMAKE_SOURCE_DIR}/utils/s3/gen_aws_service_errors.py")
set(aws_error_gen_hh
  "${scylla_gen_build_dir}/utils/s3/aws_error_definitions_generated.hh")
set(aws_error_gen_cc
  "${scylla_gen_build_dir}/utils/s3/aws_error_definitions_generated.cc")
add_custom_command(
  OUTPUT ${aws_error_gen_hh} ${aws_error_gen_cc}
  COMMAND ${Python3_EXECUTABLE} ${aws_error_gen_script}
          --output-dir ${scylla_gen_build_dir}
  DEPENDS
    ${aws_error_gen_script}
    ${CMAKE_CURRENT_SOURCE_DIR}/s3/aws_error_definitions.hh.in
    ${CMAKE_CURRENT_SOURCE_DIR}/s3/aws_error_definitions.cc.in
  COMMENT "Generating AWS service error definitions")
add_custom_target(aws-service-errors-gen
  DEPENDS ${aws_error_gen_hh} ${aws_error_gen_cc})

add_library(utils STATIC)
target_sources(utils
  PRIVATE
    UUID_gen.cc
    alien_worker.cc
    array-search.cc
    ascii.cc
    base64.cc
    big_decimal.cc
    chunked_string.cc
    bloom_calculations.cc
    bloom_filter.cc
    build_id.cc
    config_file.cc
    crypt_sha512.cc
    directories.cc
    disk-error-handler.cc
    disk_space_monitor.cc
    dynamic_bitset.cc
    error_injection.cc
    exceptions.cc
    file_lock.cc
    gz/crc_combine.cc
    gz/crc_combine_table.cc
    hashers.cc
    histogram_metrics_helper.cc
    http.cc
    http_client_error_processing.cc
    human_readable.cc
    i_filter.cc
    io-wrappers.cc
    large_bitset.cc
    like_matcher.cc
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
    labels.cc
    aws_sigv4.cc
    rest/client.cc
    s3/aws_error.cc
    ${aws_error_gen_cc}
    s3/client.cc
    s3/default_aws_retry_strategy.cc
    s3/credentials_providers/aws_credentials_provider.cc
    s3/credentials_providers/environment_aws_credentials_provider.cc
    s3/credentials_providers/instance_profile_credentials_provider.cc
    s3/credentials_providers/sts_assume_role_credentials_provider.cc
    s3/credentials_providers/aws_credentials_provider_chain.cc
    s3/utils/manip_s3.cc
    azure/identity/credentials.cc
    azure/identity/service_principal_credentials.cc
    azure/identity/managed_identity_credentials.cc
    azure/identity/azure_cli_credentials.cc
    azure/identity/default_credentials.cc
    gcp/gcp_credentials.cc
    gcp/object_storage.cc
    gcp/object_storage_retry_strategy.cc
    )
target_include_directories(utils
  PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/s3
    ${RAPIDJSON_INCLUDE_DIRS}
    ${scylla_gen_build_dir})
target_link_libraries(utils
  PUBLIC
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    Boost::regex
	crypto
    cryptopp::cryptopp
    rapidxml::rapidxml
    yaml-cpp::yaml-cpp
    GnuTLS::gnutls)
if (Scylla_USE_PRECOMPILED_HEADER_USE)
  target_precompile_headers(utils REUSE_FROM scylla-precompiled-header)
endif()

check_headers(check-headers utils
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)


