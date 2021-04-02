/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include "schema.hh"
#include "reader_permit.hh"
#include "mutation_reader.hh"
#include "mutation_fragment.hh"
#include "repair/hash.hh"
#include "streaming/stream_reason.hh"
#include "database.hh"

class repair_writer : public enable_lw_shared_from_this<repair_writer> {
    schema_ptr _schema;
    reader_permit _permit;
    uint64_t _estimated_partitions;
    size_t _nr_peer_nodes;
    std::optional<future<>> _writer_done;
    std::optional<queue_reader_handle> _mq;
    // Current partition written to disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk_written_to_sstable;
    // Is current partition still open. A partition is opened when a
    // partition_start is written and is closed when a partition_end is
    // written.
    bool _partition_opened;
    streaming::stream_reason _reason;
    named_semaphore _sem{1, named_semaphore_exception_factory{"repair_writer"}};
public:
    repair_writer(
        schema_ptr schema,
        reader_permit permit,
        uint64_t estimated_partitions,
        size_t nr_peer_nodes,
        streaming::stream_reason reason);

    future<> write_start_and_mf(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf);
    void create_writer(sharded<database>& db);
    future<> write_partition_end();
    future<> do_write(lw_shared_ptr<const decorated_key_with_hash> dk, mutation_fragment mf);
    future<> write_end_of_stream();
    future<> do_wait_for_writer_done();
    future<> wait_for_writer_done();
    named_semaphore& sem() {
        return _sem;
    }
};
