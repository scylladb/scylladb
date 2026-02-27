/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/seastar.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>

#include "sstables/types.hh"

namespace sstables {

using stream_creator_fn = std::function<future<input_stream<char>>(uint64_t, uint64_t, file_input_stream_options)>;
using integrity_error_handler = std::function<void(sstring)>;

void throwing_integrity_error_handler(sstring msg);

input_stream<char> make_checksummed_file_k_l_format_input_stream(stream_creator_fn stream_creator,
                uint64_t file_len, const sstables::checksum& checksum,
                uint64_t offset, size_t len,
                class file_input_stream_options options,
                std::optional<uint32_t> digest,
                integrity_error_handler error_handler = throwing_integrity_error_handler);

input_stream<char> make_checksummed_file_m_format_input_stream(stream_creator_fn stream_creator,
                uint64_t file_len, const sstables::checksum& checksum,
                uint64_t offset, size_t len,
                class file_input_stream_options options,
                std::optional<uint32_t> digest,
                integrity_error_handler error_handler = throwing_integrity_error_handler);
}