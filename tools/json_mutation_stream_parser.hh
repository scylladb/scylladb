/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/iostream.hh>

#include "mutation/mutation_fragment_v2.hh"

#pragma once

namespace tools {

/// Parse a stream of JSON encoded mutation fragments, using the format produced
/// by scylla-sstable write.
/// The parsing is streamed, at most a single mutation fragment is kept in memory.
/// Not all data types are supported: collections, tuples, UDTs and counters are not supported.
/// See docs/operating-scylla/admin-tools/scylla-sstable.rst for the format documentation.
class json_mutation_stream_parser {
    class impl;
    std::unique_ptr<impl> _impl;

public:
    explicit json_mutation_stream_parser(schema_ptr schema, reader_permit permit, input_stream<char> istream, logger& logger);
    json_mutation_stream_parser(json_mutation_stream_parser&&) noexcept;
    ~json_mutation_stream_parser();
    future<mutation_fragment_v2_opt> operator()();
};

} // namespace tools
