/*
 * Copyright (C) 2019 ScyllaDB
 *
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

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>

#include "utils/disk-error-handler.hh"
#include "gc_clock.hh"
#include "sstables/sstables.hh"
#include "sstables/shareable_components.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/version.hh"
#include "sstables/component_type.hh"

namespace db {

class large_data_handler;
class config;

}   // namespace db

namespace gms { class feature_service; }

namespace sstables {

using schema_ptr = lw_shared_ptr<const schema>;
using shareable_components_ptr = lw_shared_ptr<shareable_components>;

static constexpr size_t default_sstable_buffer_size = 128 * 1024;

class sstables_manager {
    db::large_data_handler& _large_data_handler;
    const db::config& _db_config;
    gms::feature_service& _features;

public:
    explicit sstables_manager(db::large_data_handler& large_data_handler, const db::config& dbcfg, gms::feature_service& feat);

    // Constructs a shared sstable
    shared_sstable make_sstable(schema_ptr schema,
            sstring dir,
            int64_t generation,
            sstable_version_types v,
            sstable_format_types f,
            gc_clock::time_point now = gc_clock::now(),
            io_error_handler_gen error_handler_gen = default_io_error_handler_gen(),
            size_t buffer_size = default_sstable_buffer_size);

    sstable_writer_config configure_writer() const;
    const db::config& config() const { return _db_config; }

    sstables::sstable::version_types get_highest_supported_format() const;

private:
    db::large_data_handler& get_large_data_handler() const {
        return _large_data_handler;
    }
};

}   // namespace sstables
