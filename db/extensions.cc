/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "extensions.hh"
#include "sstables/sstables.hh"
#include "commitlog/commitlog_extensions.hh"
#include "schema/schema.hh"
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>

db::extensions::extensions()
{}
db::extensions::~extensions()
{}

std::vector<sstables::file_io_extension*>
db::extensions::sstable_file_io_extensions() const {
    using etype = sstables::file_io_extension;
    return boost::copy_range<std::vector<etype*>>(
            _sstable_file_io_extensions
            | boost::adaptors::map_values
            | boost::adaptors::transformed(std::mem_fn(&std::unique_ptr<etype>::get)));
}

std::vector<db::commitlog_file_extension*>
db::extensions::commitlog_file_extensions() const {
    using etype = db::commitlog_file_extension;
    return boost::copy_range<std::vector<etype*>>(
            _commitlog_file_extensions
            | boost::adaptors::map_values
            | boost::adaptors::transformed(std::mem_fn(&std::unique_ptr<etype>::get)));
}

std::set<sstring>
db::extensions::schema_extension_keywords() const {
    return boost::copy_range<std::set<sstring>>(
            _schema_extensions | boost::adaptors::map_keys);
}

void db::extensions::add_sstable_file_io_extension(sstring n, sstable_file_io_extension f) {
    _sstable_file_io_extensions[n] = std::move(f);
}

void db::extensions::add_commitlog_file_extension(sstring n, commitlog_file_extension_ptr f) {
    _commitlog_file_extensions[n] = std::move(f);
}

void db::extensions::add_extension_to_schema(schema_ptr s, const sstring& name, shared_ptr<schema_extension> ext) {
    const_cast<schema *>(s.get())->extensions()[name] = std::move(ext);
}

void db::extensions::add_extension_internal_keyspace(std::string ks) {
    _extension_internal_keyspaces.emplace(std::move(ks));
}

bool db::extensions::is_extension_internal_keyspace(const std::string& ks) const {
    if (_extension_internal_keyspaces.count(ks)) {
        return true;
    }
    return false;
}
