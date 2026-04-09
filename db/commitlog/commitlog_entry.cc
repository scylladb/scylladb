/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "mutation/counters.hh"
#include "commitlog_entry.hh"
#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "db/commitlog/commitlog.hh"

#include <seastar/core/simple-stream.hh>

template<typename Output>
void commitlog_entry_writer::serialize(Output& out) const {
    [this, wr = ser::writer_of_commitlog_entry<Output>(out)] () mutable {
        if (_with_schema) {
            return std::move(wr).write_mapping(_schema->get_column_mapping());
        } else {
            return std::move(wr).skip_mapping();
        }
    }().write_mutation(_mutation).end_commitlog_entry();
}

void commitlog_entry_writer::compute_size() {
    seastar::measuring_output_stream ms;
    serialize(ms);
    _size = ms.size();
}

size_t commitlog_entry_writer::size(uint32_t segment_version) const {
    return _size + (segment_version >= db::commitlog::descriptor::segment_version_5 ? sizeof(uint8_t) : 0);
}

void commitlog_entry_writer::write(ostream& out) const {
    serialize(out);
}

void commitlog_entry_writer::write(ostream& out, uint32_t segment_version) const {
    if (segment_version >= db::commitlog::descriptor::segment_version_5) {
        ser::serialize(out, uint8_t(0));
    }
    serialize(out);
}

commitlog_entry_reader::commitlog_entry_reader(const fragmented_temporary_buffer& buffer)
    : commitlog_entry_reader(buffer, db::commitlog::descriptor::segment_version_4)
{
}

commitlog_entry_reader::commitlog_entry_reader(const fragmented_temporary_buffer& buffer, uint32_t segment_version) {
    auto in = seastar::fragmented_memory_input_stream(fragmented_temporary_buffer::view(buffer).begin(), buffer.size_bytes());
    if (segment_version >= db::commitlog::descriptor::segment_version_5 && buffer.size_bytes() >= sizeof(uint8_t)) {
        uint8_t entry_type = 0;
        in.read(reinterpret_cast<char*>(&entry_type), sizeof(entry_type));
        if (entry_type == 0) {
            _ce.emplace(ser::deserialize(in, std::type_identity<commitlog_entry>()));
        } else if (entry_type == 1) {
            _raft_ce.emplace(ser::deserialize(in, std::type_identity<raft_commit_log_entry>()));
        } else {
            throw std::runtime_error(format("Unknown v5 commitlog entry type {}", entry_type));
        }
    } else {
        _ce.emplace(ser::deserialize(in, std::type_identity<commitlog_entry>()));
    }
}

const std::optional<column_mapping>& commitlog_entry_reader::get_column_mapping() const {
    static const std::optional<column_mapping> none{};
    if (!_ce) {
        return none;
    }
    return _ce->mapping();
}

const frozen_mutation& commitlog_entry_reader::mutation() const & {
    if (!_ce) {
        throw std::logic_error("commitlog entry does not contain mutation payload");
    }
    return _ce->mutation();
}

frozen_mutation&& commitlog_entry_reader::mutation() && {
    if (!_ce) {
        throw std::logic_error("commitlog entry does not contain mutation payload");
    }
    return std::move(*_ce).mutation();
}
