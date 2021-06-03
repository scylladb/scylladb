/*
 * Copyright (C) 2019-present ScyllaDB
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

#include "log.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstables.hh"
#include "db/config.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"

namespace sstables {

logging::logger smlogger("sstables_manager");

sstables_manager::sstables_manager(
    db::large_data_handler& large_data_handler, const db::config& dbcfg, gms::feature_service& feat)
    : _large_data_handler(large_data_handler), _db_config(dbcfg), _features(feat) {
}

sstables_manager::~sstables_manager() {
    assert(_closing);
    assert(_active.empty());
    assert(_undergoing_close.empty());
}

shared_sstable sstables_manager::make_sstable(schema_ptr schema,
        sstring dir,
        int64_t generation,
        sstable_version_types v,
        sstable_format_types f,
        gc_clock::time_point now,
        io_error_handler_gen error_handler_gen,
        size_t buffer_size) {
    return make_lw_shared<sstable>(std::move(schema), std::move(dir), generation, v, f, get_large_data_handler(), *this, now, std::move(error_handler_gen), buffer_size);
}

sstable_writer_config sstables_manager::configure_writer(sstring origin) const {
    sstable_writer_config cfg;

    cfg.promoted_index_block_size = _db_config.column_index_size_in_kb() * 1024;
    cfg.validation_level = _db_config.enable_sstable_key_validation()
            ? mutation_fragment_stream_validation_level::clustering_key
            : mutation_fragment_stream_validation_level::token;
    cfg.summary_byte_cost = summary_byte_cost(_db_config.sstable_summary_ratio());

    cfg.origin = std::move(origin);

    return cfg;
}

void sstables_manager::add(sstable* sst) {
    _active.push_back(*sst);
}

void sstables_manager::deactivate(sstable* sst) {
    // At this point, sst has a reference count of zero, since we got here from
    // lw_shared_ptr_deleter<sstables::sstable>::dispose().
    _active.erase(_active.iterator_to(*sst));
    _undergoing_close.push_back(*sst);
    // guard against sstable::close_files() calling shared_from_this() and immediately destroying
    // the result, which will dispose of the sstable recursively
    auto ptr = sst->shared_from_this();
    (void)sst->close_files().finally([ptr] {
        // destruction of ptr will call maybe_done() and release close()
    });
}

void sstables_manager::remove(sstable* sst) {
    _undergoing_close.erase(_undergoing_close.iterator_to(*sst));
    delete sst;
    maybe_done();
}

void sstables_manager::maybe_done() {
    if (_closing && _active.empty() && _undergoing_close.empty()) {
        _done.set_value();
    }
}

future<> sstables_manager::close() {
    _closing = true;
    maybe_done();
    return _done.get_future();
}

}   // namespace sstables
