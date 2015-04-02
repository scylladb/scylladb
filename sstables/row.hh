/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 *
 */

#pragma once

#include "bytes.hh"
#include "core/temporary_buffer.hh"

// sstables::data_consume_row feeds the contents of a single row into a
// row_consumer object:
//
// * First, consume_row_start() is called, with some information about the
//   whole row: The row's key, timestamp, etc.
// * Next, consume_cell() is called once for every column.
// * Finally, consume_row_end() is called. A consumer written for a single
//   column will likely not want to do anything here.
//
// Important note: the row key, column name and column value, passed to the
// consume_* functions, are passed as a "bytes_view" object, which points to
// internal data held by the feeder. This internal data is only valid for the
// duration of the single consume function it was passed to. If the object
// wants to hold these strings longer, it must make a copy of the bytes_view's
// contents. [Note, in reality, because our implementation reads the whole
// row into one buffer, the byte_views remain valid until consume_row_end()
// is called.]
class row_consumer {
public:
    // Consume the row's key and deletion_time. The latter determines if the
    // row is a tombstone, and if so, when it has been deleted.
    // Note that the key is in serialized form, and should be deserialized
    // (according to the schema) before use.
    // As explained above, the key object is only valid during this call, and
    // if the implementation wishes to save it, it must copy the *contents*.
    virtual void consume_row_start(bytes_view key, sstables::deletion_time deltime) = 0;

    // Consume one cell (column name and value). Both are serialized, and need
    // to be deserialized according to the schema.
    virtual void consume_cell(bytes_view col_name, bytes_view value, uint64_t timestamp) = 0;

    // Called at the end of the row, after all cells.
    virtual void consume_row_end() = 0;

    virtual ~row_consumer() { }
};
