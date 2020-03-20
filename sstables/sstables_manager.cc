/*
 * Copyright (C) 2019 ScyllaDB
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
#include "service/storage_service.hh" // To be removed soon

namespace sstables {

logging::logger smlogger("sstables_manager");

sstables_manager::sstables_manager(
    db::large_data_handler& large_data_handler, const db::config& dbcfg, gms::feature_service& feat)
    : _large_data_handler(large_data_handler), _db_config(dbcfg), _features(feat) {
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

sstable_writer_config sstables_manager::configure_writer() const {
    sstable_writer_config cfg;

    cfg.promoted_index_block_size = _db_config.column_index_size_in_kb() * 1024;
    cfg.validate_keys = _db_config.enable_sstable_key_validation();
    cfg.summary_byte_cost = summary_byte_cost(_db_config.sstable_summary_ratio());

    cfg.correctly_serialize_non_compound_range_tombstones =
            _features.cluster_supports_reading_correctly_serialized_range_tombstones();
    cfg.correctly_serialize_static_compact_in_mc =
            bool(_features.cluster_supports_correct_static_compact_in_mc());

    return cfg;
}

sstables::sstable::version_types sstables_manager::get_highest_supported_format() const {
    return service::get_local_storage_service().sstables_format();
}

}   // namespace sstables
