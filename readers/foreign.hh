/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>

#include "readers/mutation_reader_fwd.hh"
#include "schema/schema_fwd.hh"

class reader_permit;

/// Make a foreign_reader.
///
/// foreign_reader is a local representant of a reader located on a remote
/// shard. Manages its lifecycle and takes care of seamlessly transferring
/// produced fragments. Fragments are *copied* between the shards, a
/// bufferful at a time.
/// To maximize throughput read-ahead is used. After each fill_buffer() or
/// fast_forward_to() a read-ahead (a fill_buffer() on the remote reader) is
/// issued. This read-ahead runs in the background and is brought back to
/// foreground on the next fill_buffer() or fast_forward_to() call.
/// If the reader resides on this shard (the shard where make_foreign_reader()
/// is called) there is no need to wrap it in foreign_reader, just return it as
/// is.
mutation_reader make_foreign_reader(schema_ptr schema,
        reader_permit permit,
        foreign_ptr<std::unique_ptr<mutation_reader>> reader,
        streamed_mutation::forwarding fwd_sm = streamed_mutation::forwarding::no);
