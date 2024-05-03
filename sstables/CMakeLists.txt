add_library(sstables STATIC)
target_sources(sstables
  PRIVATE
    compress.cc
    integrity_checked_file_impl.cc
    kl/reader.cc
    metadata_collector.cc
    m_format_read_helpers.cc
    mx/partition_reversing_data_source.cc
    mx/reader.cc
    mx/writer.cc
    prepended_input_stream.cc
    random_access_reader.cc
    sstable_directory.cc
    sstable_mutation_reader.cc
    sstables.cc
    sstable_set.cc
    sstables_manager.cc
    sstable_version.cc
    storage.cc
    writer.cc)
target_include_directories(sstables
  PUBLIC
    ${CMAKE_SOURCE_DIR})
target_link_libraries(sstables
  PUBLIC
    idl
    wasmtime_bindings
    Seastar::seastar
    xxHash::xxhash
  PRIVATE
    readers
    tracing
    absl::headers
    libdeflate::libdeflate
    ZLIB::ZLIB)

check_headers(check-headers streaming
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
