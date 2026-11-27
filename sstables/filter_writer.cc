/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/filter_writer.hh"
#include "sstables/sstables.hh"
#include "sstables/writer.hh"
#include "schema/schema.hh"

namespace sstables {

filter_writer::filter_writer(sstable& sst, uint64_t estimated_partitions)
        : _sst(sst)
        , _delayed_filter(_sst.has_component(component_type::Filter) && !_sst.has_component(component_type::Index)) {
    if (_delayed_filter) {
        file_output_stream_options options;
        options.buffer_size = 32 * 1024;
        _hashes_writer = std::make_unique<file_writer>(_sst.make_component_file_writer(component_type::TemporaryHashes, std::move(options),
            sstable_write_open_flags).get());
    } else {
        _sst._components->filter = utils::i_filter::get_filter(estimated_partitions, _sst._schema->bloom_filter_fp_chance(), utils::filter_format::m_format);
    }
}

filter_writer::~filter_writer() {
    if (_hashes_writer) {
        try {
            _hashes_writer->close();
        } catch (...) {
            sstlog.error("writer failed to close file: {:t}", std::current_exception());
        }
    }
}

void filter_writer::add(bytes_view partition_key) {
    _current_hash = utils::make_hashed_key(partition_key);
    if (_hashes_writer) {
        std::array<uint64_t, 2> hash = {
            seastar::cpu_to_le(_current_hash.hash()[0]),
            seastar::cpu_to_le(_current_hash.hash()[1])
        };
        _hashes_writer->write(reinterpret_cast<const char*>(hash.data()), sizeof(hash));
    } else {
        _sst._components->filter->add(_current_hash);
    }
    _num_partitions++;
}

void filter_writer::finish() {
    if (_hashes_writer) {
        auto writer = std::move(_hashes_writer);
        writer->close();
    }
    if (_delayed_filter) {
        _sst.build_delayed_filter(_num_partitions);
    } else {
        _sst.maybe_rebuild_filter_from_index(_num_partitions);
    }
    _sst.write_filter();
}

} // namespace sstables
