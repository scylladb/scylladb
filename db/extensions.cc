/*
 * Copyright (C) 2015 ScyllaDB
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

#include "extensions.hh"
#include "sstables/sstables.hh"
#include "commitlog/commitlog_extensions.hh"
#include "schema.hh"

db::extensions::extensions()
{}
db::extensions::~extensions()
{}

void db::extensions::add_sstable_file_io_extension(sstring n, sstable_file_io_extension f) {
    _sstable_file_io_extensions[n] = std::move(f);
}

void db::extensions::add_commitlog_file_extension(sstring n, commitlog_file_extension_ptr f) {
    _commitlog_file_extensions[n] = std::move(f);
}

void db::extensions::add_extension_to_schema(schema_ptr s, const sstring& name, shared_ptr<schema_extension> ext) {
    const_cast<schema *>(s.get())->extensions()[name] = std::move(ext);
}
