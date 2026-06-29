/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "mutation/counters.hh"
#include "commitlog_entry.hh"
#include "idl/commitlog.dist.hh"
#include "idl/commitlog.dist.impl.hh"
#include "idl/mutation.dist.hh"
#include "idl/mutation.dist.impl.hh"
#include "idl/raft_storage.dist.hh"
#include "idl/raft_storage.dist.impl.hh"
#include <seastar/core/simple-stream.hh>

template<typename Output>
void commitlog_mutation_entry_writer::serialize(Output& out) const {
    auto write_mut = [this](auto wr) {
        auto after_mapping = _with_schema
            ? std::move(wr).write_mapping(_schema->get_column_mapping())
            : std::move(wr).skip_mapping();
        return std::move(after_mapping).write_mutation(_mutation).end_mutation_entry();
    };
    if (use_variant_commitlog_entry_format()) {
        write_mut(ser::writer_of_commitlog_entry<Output>(out).start_item_mutation_entry()).end_commitlog_entry();
    } else {
        write_mut(ser::writer_of_mutation_entry<Output>(out));
    }
}

void commitlog_mutation_entry_writer::write(ostream& out) const {
    serialize(out);
}

void commitlog_mutation_entry_writer::compute_size() {
    seastar::measuring_output_stream ms;
    serialize(ms);
    _size = ms.size();
}


template<typename Output>
inline void commitlog_raft_log_entry_writer::serialize(Output& out) const {
    auto writer = ser::writer_of_commitlog_entry<Output>(out);
    std::visit([&] (const auto& item) {
        using item_type = std::decay_t<decltype(item)>;
        if constexpr (std::is_same_v<item_type, raft_commitlog_entry>) {
            std::move(writer).write_item_raft_commitlog_entry(item).end_commitlog_entry();
        } else {
            std::move(writer).write_item_raft_commitlog_entry_with_commit_idx(item).end_commitlog_entry();
        }
    }, _item);
}

void commitlog_raft_log_entry_writer::write(ostream& out) const {
    serialize(out);
}

namespace {
auto read_variant_commitlog_entry(const fragmented_temporary_buffer& buffer) {
    // XXX this is copying of the data, but since this is not critical path, only used during replay from commitlog,
    // we can tolerate this for now. Should be fixed when we fix IDL generator to support deserialization directly from
    // fragmented buffers.
    bytes_ostream bo;
    for (bytes_view frag : fragment_range(fragmented_temporary_buffer::view(buffer))) {
        bo.write(reinterpret_cast<const char*>(frag.data()), frag.size());
    }
    auto in = ser::as_input_stream(bo);

    // Note: we use the 'view' deserializer here because calling
    // `ser::deserialize(in, std::type_identity<ser::commitlog_entry>())`
    // would use the "manual" deserializer, which is not binary-compatible
    // with the format produced by `ser::writer_of_commitlog_entry`.
    // See issue: https://scylladb.atlassian.net/browse/SCYLLADB-1029
    //
    // For the same reason, we cannot rely on the implicit conversion from
    // `ser::commitlog_entry_view` to `commitlog_entry` using
    // `operator commitlog_entry()` since that conversion
    // is also implemented using the "manual" deserializer.
    auto view = ser::deserialize(in, std::type_identity<ser::commitlog_entry_view>());
    return seastar::visit(
            view.item(),
            [](raft_commitlog_entry raft_log_entry) {
                return commitlog_entry{.item = std::move(raft_log_entry)};
            },
            [](raft_commitlog_entry_with_commit_idx raft_log_entry) {
                return commitlog_entry{.item = std::move(raft_log_entry)};
            },
            [](const ser::mutation_entry_view& entry_view) {
                return commitlog_entry{.item = mutation_entry(entry_view.mapping(), entry_view.mutation())};
            },
            [](ser::unknown_variant_type) -> commitlog_entry {
                throw std::runtime_error("Unknown variant type in commitlog entry");
            });
}
auto read_mutation_commitlog_entry(const fragmented_temporary_buffer& buffer) {
    auto in = seastar::fragmented_memory_input_stream(fragmented_temporary_buffer::view(buffer).begin(), buffer.size_bytes());
    auto e = ser::deserialize(in, std::type_identity<mutation_entry>());
    return commitlog_entry{.item = mutation_entry(e.mapping(), frozen_mutation(e.mutation()))};
}
} // namespace

commitlog_entry_reader::commitlog_entry_reader(const fragmented_temporary_buffer& buffer, detail::commitlog_entry_serialization_format format)
    : _entry(format == detail::commitlog_entry_serialization_format::variant ? read_variant_commitlog_entry(buffer) : read_mutation_commitlog_entry(buffer)) {
}


commitlog_mutation_entry_reader::commitlog_mutation_entry_reader(const fragmented_temporary_buffer& buffer)
    : _me([&] {
        auto in = seastar::fragmented_memory_input_stream(fragmented_temporary_buffer::view(buffer).begin(), buffer.size_bytes());
        return ser::deserialize(in, std::type_identity<mutation_entry>());
    }()) {
}

void commitlog_raft_log_entry_writer::compute_size() {
    seastar::measuring_output_stream ms;
    serialize(ms);
    _size = ms.size();
}
