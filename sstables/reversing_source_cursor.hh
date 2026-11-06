/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <memory>

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include "seastarx.hh"
#include "sstables/consumer.hh"
#include "sstables/sstable_position.hh"

namespace sstables {

// The I/O interface for the partition-reversing data source. For logical
// positions, it uses sstable::data_stream() and sstable::data_read(). For
// physical positions, it uses one bidirectional decompressing_cursor.
//
// This interface is not an input stream. A parser must own its forward stream,
// but reverse row reads operate directly on the cursor. The logical
// implementation makes an independent stream for each parser. The physical
// implementation gives each parser a borrowed view. Forward and backward reads
// then share the same physical position and buffers.
class reversing_source_cursor {
public:
    virtual ~reversing_source_cursor() = default;

    // Open a forward stream over [start, end). The parser owns and closes the
    // returned object. The object can be a non-owning view of a shared cursor.
    virtual future<std::unique_ptr<data_consumer::continuous_data_consumer_input_stream>>
    make_forward_input_stream(sstable_position start, sstable_position end) = 0;

    // Read the uncompressed bytes in [start, end) in file order. This operation
    // reads the partition header in its original order. Returns fewer bytes
    // if the file ends before `end`.
    virtual future<temporary_buffer<char>>
    read_forwards(sstable_position start, sstable_position end) = 0;

    struct backward_read {
        // The position of the first returned byte.
        sstable_position start;
        // The bytes in file order. The caller can modify them.
        temporary_buffer<char> data;
    };

    // Read the `size` uncompressed bytes that end at `end`, while the cursor
    // moves backward. Physical positions can't be subtracted, so the start of
    // the bytes is known only after they are read; it is returned with them.
    virtual future<backward_read>
    read_backwards(sstable_position end, size_t size) = 0;

    // The read range shrank, so the backward walk jumps down to `new_end`,
    // which is below its current position. Keep the read-ahead below
    // `new_end` and discard the rest. Cheap; safe to call between backward
    // reads.
    virtual void trim_read_ahead(sstable_position new_end) = 0;

    virtual future<> close() = 0;
};

} // namespace sstables
