/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/serialization_header_builder.hh"
#include "encoding_stats.hh"
#include "schema/schema.hh"

namespace sstables {

static bytes_array_vint_size to_bytes_array_vint_size(bytes b) {
    bytes_array_vint_size result;
    result.value = std::move(b);
    return result;
}

static bytes_array_vint_size to_bytes_array_vint_size(const sstring& s) {
    bytes_array_vint_size result;
    result.value = to_bytes(s);
    return result;
}

static sstring pk_type_to_string(const schema& s) {
    if (s.partition_key_size() == 1) {
        return s.partition_key_columns().begin()->type->name();
    } else {
        return seastar::format("org.apache.cassandra.db.marshal.CompositeType({})",
                               fmt::join(s.partition_key_columns()
                                          | std::views::transform(std::mem_fn(&column_definition::type))
                                          | std::views::transform(std::mem_fn(&abstract_type::name)),
                                        ","));
    }
}

serialization_header make_serialization_header(const schema& s, const encoding_stats& enc_stats) {
    serialization_header header;
    // mc serialization header minimum values are delta-encoded based on the default timestamp epoch times
    // Note: We rely on implicit conversion to uint64_t when subtracting the signed epoch values below
    // for preventing signed integer overflow.
    header.min_timestamp_base.value = static_cast<uint64_t>(enc_stats.min_timestamp) - encoding_stats::timestamp_epoch;
    header.min_local_deletion_time_base.value = static_cast<uint64_t>(enc_stats.min_local_deletion_time.time_since_epoch().count()) - encoding_stats::deletion_time_epoch;
    header.min_ttl_base.value = static_cast<uint64_t>(enc_stats.min_ttl.count()) - encoding_stats::ttl_epoch;

    header.pk_type_name = to_bytes_array_vint_size(pk_type_to_string(s));

    header.clustering_key_types_names.elements.reserve(s.clustering_key_size());
    for (const auto& ck_column : s.clustering_key_columns()) {
        auto ck_type_name = to_bytes_array_vint_size(ck_column.type->name());
        header.clustering_key_types_names.elements.push_back(std::move(ck_type_name));
    }

    for (const auto& column : s.v3().all_columns()) {
        serialization_header::column_desc cd;
        cd.name = to_bytes_array_vint_size(column.name());
        cd.type_name = to_bytes_array_vint_size(column.type->name());
        if (column.is_static()) {
            header.static_columns.elements.push_back(std::move(cd));
        } else if (column.is_regular()) {
            header.regular_columns.elements.push_back(std::move(cd));
        }
    }

    return header;
}

} // namespace sstables
