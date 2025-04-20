/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sstable_mutation_reader.hh"

#include "column_translation.hh"
#include "concrete_types.hh"
#include "utils/fragment_range.hh"
#include "utils/to_string.hh"

#include <boost/intrusive/list.hpp>

namespace sstables {

class reader_tracker {
    using list_type = boost::intrusive::list<mp_row_consumer_reader_base,
        boost::intrusive::member_hook<mp_row_consumer_reader_base,
            mp_row_consumer_reader_base::tracker_link_type,
            &mp_row_consumer_reader_base::_tracker_link>,
        boost::intrusive::constant_time_size<false>>;
public:
    list_type _readers;

    void add(mp_row_consumer_reader_base& reader) {
        _readers.push_back(reader);
    }
};

thread_local reader_tracker _reader_tracker;

mp_row_consumer_reader_base::mp_row_consumer_reader_base(shared_sstable sst)
    : _sst(std::move(sst))
{
    _reader_tracker.add(*this);
}

atomic_cell make_counter_cell(api::timestamp_type timestamp, fragmented_temporary_buffer::view cell_value) {
    static constexpr size_t shard_size = 32;

    if (cell_value.empty()) {
        // This will never happen in a correct MC sstable but
        // we had a bug #4363 that caused empty counters
        // to be incorrectly stored inside sstables.
        counter_cell_builder ccb;
        return ccb.build(timestamp);
    }

    auto cell_value_size = cell_value.size_bytes();

    auto header_size = read_simple<int16_t>(cell_value);
    for (auto i = 0; i < header_size; i++) {
        auto idx = read_simple<int16_t>(cell_value);
        if (idx >= 0) {
            throw marshal_exception("encountered a local shard in a counter cell");
        }
    }
    auto header_length = (size_t(header_size) + 1) * sizeof(int16_t);
    auto shard_count = (cell_value_size - header_length) / shard_size;
    if (shard_count != size_t(header_size)) {
        throw marshal_exception("encountered remote shards in a counter cell");
    }

    counter_cell_builder ccb(shard_count);
    for (auto i = 0u; i < shard_count; i++) {
        auto id_hi = read_simple<int64_t>(cell_value);
        auto id_lo = read_simple<int64_t>(cell_value);
        auto clock = read_simple<int64_t>(cell_value);
        auto value = read_simple<int64_t>(cell_value);
        ccb.add_maybe_unsorted_shard(counter_shard(counter_id(utils::UUID(id_hi, id_lo)), value, clock));
    }
    ccb.sort_and_remove_duplicates();
    return ccb.build(timestamp);
}

// See #6130.
static data_type freeze_types_in_collections(data_type t) {
    return ::visit(*t, make_visitor(
    [] (const map_type_impl& typ) -> data_type {
        return map_type_impl::get_instance(
                freeze_types_in_collections(typ.get_keys_type()->freeze()),
                freeze_types_in_collections(typ.get_values_type()->freeze()),
                typ.is_multi_cell());
    },
    [] (const set_type_impl& typ) -> data_type {
        return set_type_impl::get_instance(
                freeze_types_in_collections(typ.get_elements_type()->freeze()),
                typ.is_multi_cell());
    },
    [] (const list_type_impl& typ) -> data_type {
        return list_type_impl::get_instance(
                freeze_types_in_collections(typ.get_elements_type()->freeze()),
                typ.is_multi_cell());
    },
    [&] (const abstract_type& typ) -> data_type {
        return std::move(t);
    }
    ));
}

/* If this function returns false, the caller cannot assume that the SSTable comes from Scylla.
 * It might, if for some reason a table was created using Scylla that didn't contain any feature bit,
 * but that should never happen. */
static bool is_certainly_scylla_sstable(const sstable_enabled_features& features) {
    return features.enabled_features;
}

std::vector<column_translation::column_info> column_translation::state::build(
        const schema& s,
        const utils::chunked_vector<serialization_header::column_desc>& src,
        const sstable_enabled_features& features,
        bool is_static) {
    std::vector<column_info> cols;
    if (s.is_dense()) {
        const column_definition& col = is_static ? *s.static_begin() : *s.regular_begin();
        cols.push_back(column_info{
            &col.name(),
            col.type,
            col.id,
            col.type->value_length_if_fixed(),
            col.is_multi_cell(),
            col.is_counter(),
            false
        });
    } else {
        cols.reserve(src.size());
        for (auto&& desc : src) {
            const bytes& type_name = desc.type_name.value;
            data_type type = db::marshal::type_parser::parse(to_string_view(type_name));
            if (!features.is_enabled(CorrectUDTsInCollections) && is_certainly_scylla_sstable(features)) {
                // See #6130.
                type = freeze_types_in_collections(std::move(type));
            }
            const column_definition* def = s.get_column_definition(desc.name.value);
            std::optional<column_id> id;
            bool schema_mismatch = false;
            if (def) {
                id = def->id;
                schema_mismatch = def->is_multi_cell() != type->is_multi_cell() ||
                                  def->is_counter() != type->is_counter() ||
                                  !def->type->is_value_compatible_with(*type);
            }
            cols.push_back(column_info{
                &desc.name.value,
                type,
                id,
                type->value_length_if_fixed(),
                type->is_multi_cell(),
                type->is_counter(),
                schema_mismatch
            });
        }
        std::ranges::stable_partition(cols, [](const column_info& column) { return !column.is_collection; });
    }
    return cols;
}



position_in_partition_view get_slice_upper_bound(const schema& s, const query::partition_slice& slice, dht::ring_position_view key) {
    const auto& ranges = slice.row_ranges(s, *key.key());
    if (ranges.empty()) {
        return position_in_partition_view::for_static_row();
    }
    if (slice.is_reversed()) {
        return position_in_partition_view::for_range_end(ranges.front());
    }
    return position_in_partition_view::for_range_end(ranges.back());
}

position_in_partition_view get_slice_lower_bound(const schema& s, const query::partition_slice& slice, dht::ring_position_view key) {
    const auto& ranges = slice.row_ranges(s, *key.key());
    if (ranges.empty()) {
        return position_in_partition_view::for_static_row();
    }
    if (slice.is_reversed()) {
        return position_in_partition_view::for_range_start(ranges.back());
    }
    return position_in_partition_view::for_range_start(ranges.front());
}

} // namespace sstables
