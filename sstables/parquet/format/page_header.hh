/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// PageHeader, the Thrift struct prefixed to every page in a column chunk.
//
// V2 data pages are the interesting case for us: their definition and
// repetition level streams are never compressed, so a reader can decode levels
// (and therefore nulls, and therefore row alignment) without invoking a codec
// at all. That is what makes page-level null-skipping cheap.

#include "thrift_compact.hh"
#include "parquet_metadata.hh"

#include <optional>
#include <span>

namespace sstables::parquet::format {

enum class page_type : int32_t {
    data_page = 0, index_page = 1, dictionary_page = 2, data_page_v2 = 3,
};

struct data_page_header {
    int32_t  num_values = 0;
    encoding value_encoding{};
    encoding definition_level_encoding{};
    encoding repetition_level_encoding{};
};

struct data_page_header_v2 {
    int32_t  num_values = 0;
    int32_t  num_nulls = 0;
    int32_t  num_rows = 0;
    encoding value_encoding{};
    int32_t  definition_levels_byte_length = 0;
    int32_t  repetition_levels_byte_length = 0;
    bool     is_compressed = true;   // spec default
};

struct dictionary_page_header {
    int32_t  num_values = 0;
    encoding value_encoding{};
};

struct page_header {
    page_type type{};
    int32_t   uncompressed_page_size = 0;
    int32_t   compressed_page_size = 0;
    std::optional<int32_t>               crc;
    std::optional<data_page_header>      v1;
    std::optional<data_page_header_v2>   v2;
    std::optional<dictionary_page_header> dict;
};

// Parses a page header from the front of `buf`. On return, `consumed` holds the
// number of bytes the header occupied, so the caller can find the page body.
page_header parse_page_header(std::span<const uint8_t> buf, size_t& consumed, limits = {});

} // namespace sstables::parquet::format
