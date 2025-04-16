/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/util/noncopyable_function.hh>
#include "seastarx.hh"

/**
 * Creates a write-only file wrapping a data_sink.
 * 
 * The resulting file object can do sequential 
 * writes only. It implements truncate, but is limited 
 * to [<current write pos> - <write block size>] for
 * truncation position.
 * 
 * Essentially, it is only really fit for
 * using as wrapped in a file_data_sink_impl under
 * an output_stream.
 */
seastar::file create_file_for_sink(seastar::data_sink);

/**
 * Creates a file that can only do flush() and close().
 * Do not write to it.
 */
seastar::file create_noop_file();

/**
 * Creates a data sink which will forward all data
 * sent to it into the destination vector.
 */
seastar::data_sink create_memory_sink(std::vector<seastar::temporary_buffer<char>>&);

/**
 * Creates a data source that will read data sequentially
 * from the source vector buffers.
 */
seastar::data_source create_memory_source(std::vector<seastar::temporary_buffer<char>>);
