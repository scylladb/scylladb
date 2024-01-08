/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "bytes.hh"
#include "keys.hh"
#include "paging_state.hh"
#include <seastar/core/simple-stream.hh>
#include "idl/paging_state.dist.hh"
#include "idl/paging_state.dist.impl.hh"
#include "exceptions/exceptions.hh"
#include "message/messaging_service.hh"
#include "utils/bit_cast.hh"

service::pager::paging_state::paging_state(partition_key pk,
        std::optional<clustering_key> ck,
        uint32_t rem_low_bits,
        query_id query_uuid,
        replicas_per_token_range last_replicas,
        std::optional<db::read_repair_decision> query_read_repair_decision,
        uint32_t rows_fetched_for_last_partition_low_bits,
        uint32_t rem_high_bits,
        uint32_t rows_fetched_for_last_partition_high_bits,
        bound_weight ck_weight,
        partition_region region)
    : _partition_key(std::move(pk))
    , _clustering_key(std::move(ck))
    , _remaining_low_bits(rem_low_bits)
    , _query_uuid(query_uuid)
    , _last_replicas(std::move(last_replicas))
    , _query_read_repair_decision(query_read_repair_decision)
    , _rows_fetched_for_last_partition_low_bits(rows_fetched_for_last_partition_low_bits)
    , _remaining_high_bits(rem_high_bits)
    , _rows_fetched_for_last_partition_high_bits(rows_fetched_for_last_partition_high_bits)
    , _ck_weight(ck_weight)
    , _region(region)
{ }

service::pager::paging_state::paging_state(partition_key pk,
        position_in_partition_view pos,
        uint64_t rem,
        query_id query_uuid,
        replicas_per_token_range last_replicas,
        std::optional<db::read_repair_decision> query_read_repair_decision,
        uint64_t rows_fetched_for_last_partition)
    : paging_state(std::move(pk), pos.has_key() ? std::optional(pos.key()) : std::nullopt, static_cast<uint32_t>(rem), query_uuid, std::move(last_replicas), query_read_repair_decision,
            static_cast<uint32_t>(rows_fetched_for_last_partition), static_cast<uint32_t>(rem >> 32),
            static_cast<uint32_t>(rows_fetched_for_last_partition >> 32),
            pos.get_bound_weight(),
            pos.region())
{ }

lw_shared_ptr<service::pager::paging_state> service::pager::paging_state::deserialize(
        bytes_opt data) {
    if (!data) {
        return nullptr;
    }

    if (data.value().size() < sizeof(uint32_t) || le_to_cpu(read_unaligned<int32_t>(data.value().begin())) != netw::messaging_service::current_version) {
        throw exceptions::protocol_exception("Invalid value for the paging state");
    }


    // skip 4 bytes that contain format id
    seastar::simple_input_stream in(reinterpret_cast<char*>(data.value().begin() + sizeof(uint32_t)), data.value().size() - sizeof(uint32_t));

    try {
        return make_lw_shared<paging_state>(ser::deserialize(in, boost::type<paging_state>()));
    } catch (...) {
        std::throw_with_nested(
                exceptions::protocol_exception(
                        "Invalid value for the paging state"));
    }
}

bytes_opt service::pager::paging_state::serialize() const {
    bytes b = ser::serialize_to_buffer<bytes>(*this, sizeof(uint32_t));
    // put serialization format id
    write_unaligned<int32_t>(b.begin(), cpu_to_le(netw::messaging_service::current_version));
    return {std::move(b)};
}
