/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <seastar/core/iostream.hh>
#include <seastar/core/io_priority_class.hh>
#include "reader_permit.hh"
#include "sstables/index_reader.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/sstables.hh"
#include "tracing/trace_state.hh"

namespace sstables {
namespace mx {

seastar::data_source make_partition_reversing_data_source(const schema& s, shared_sstable sst, index_reader& ir, uint64_t pos, size_t len,
                                                          reader_permit permit, const io_priority_class& io_priority, tracing::trace_state_ptr trace_state);

}
}
