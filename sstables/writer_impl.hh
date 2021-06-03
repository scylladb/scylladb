/*
 * Copyright (C) 2018-present ScyllaDB
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

#include "sstables.hh"
#include "schema_fwd.hh"
#include "mutation_fragment.hh"
#include "metadata_collector.hh"
#include "mutation_fragment_stream_validator.hh"

namespace sstables {

struct sstable_writer::writer_impl {
    sstable& _sst;
    const schema& _schema;
    const io_priority_class& _pc;
    const sstable_writer_config _cfg;
    // NOTE: _collector and _c_stats are used to generation of statistics file
    // when writing a new sstable.
    metadata_collector _collector;
    column_stats _c_stats;
    mutation_fragment_stream_validating_filter _validator;

    writer_impl(sstable& sst, const schema& schema, const io_priority_class& pc, const sstable_writer_config& cfg)
        : _sst(sst)
        , _schema(schema)
        , _pc(pc)
        , _cfg(cfg)
        , _collector(_schema, sst.get_filename())
        , _validator(format("sstable writer {}", _sst.get_filename()), _schema, _cfg.validation_level)
    {}

    virtual void consume_new_partition(const dht::decorated_key& dk) = 0;
    virtual void consume(tombstone t) = 0;
    virtual stop_iteration consume(static_row&& sr) = 0;
    virtual stop_iteration consume(clustering_row&& cr) = 0;
    virtual stop_iteration consume(range_tombstone&& rt) = 0;
    virtual stop_iteration consume_end_of_partition() = 0;
    virtual void consume_end_of_stream() = 0;
    virtual ~writer_impl() {}
};

}
