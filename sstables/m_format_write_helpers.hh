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

#include <type_traits>

#include "bytes.hh"
#include "types.hh"
#include "timestamp.hh"

class schema;
class row;
class clustering_key_prefix;
class encoding_stats;

namespace sstables {

class file_writer;

// Utilities for writing integral values in variable-length format
// See vint-serialization.hh for more details
void write_unsigned_vint(file_writer& out, uint64_t value);
void write_signed_vint(file_writer& out, int64_t value);

template <typename T>
typename std::enable_if_t<!std::is_integral_v<T>>
write_vint(file_writer& out, T t) = delete;

template <typename T>
inline void write_vint(file_writer& out, T value) {
    static_assert(std::is_integral_v<T>, "Non-integral values can't be written using write_vint");
    return std::is_unsigned_v<T> ? write_unsigned_vint(out, value) : write_signed_vint(out, value);
}


// Writes clustering prefix, full or not, encoded in SSTables 3.0 format
void write_clustering_prefix(file_writer& out, const schema& s, const clustering_key_prefix& prefix);

// Writes encoded information about missing columns in the given row
void write_missing_columns(file_writer& out, const schema& s, const row& row);

// Helper functions for writing delta-encoded time-related values
void write_delta_timestamp(file_writer& out, api::timestamp_type timestamp, const encoding_stats& enc_stats);

void write_delta_ttl(file_writer& out, uint32_t ttl, const encoding_stats& enc_stats);

void write_delta_local_deletion_time(file_writer& out, uint32_t local_deletion_time, const encoding_stats& enc_stats);

void write_delta_deletion_time(file_writer& out, deletion_time dt, const encoding_stats& enc_stats);

};   // namespace sstables
