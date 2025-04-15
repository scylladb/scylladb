/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/shared_dict.hh"
#include "utils/advanced_rpc_compressor.hh"

namespace utils {

struct control_protocol_frame {
    enum header_enum : uint8_t {
        NONE, // This means that the field isn't filled.
        UPDATE,
        COMMIT,
    };

    struct one_side {
        header_enum header = NONE;
        uint64_t epoch = 0;
        compression_algorithm_set algo = compression_algorithm_set::singleton(compression_algorithm::type::RAW);
        shared_dict::dict_id dict;
        constexpr static size_t serialized_size = 1+8+1+16+8+32;
        void serialize(std::span<std::byte, serialized_size>);
        static one_side deserialize(std::span<const std::byte, serialized_size>);
    };

    // The negotiation algorithm is run for each of the two directions of the connection separately.
    // The `receiver` field below is a message for the algorithm instance in which we are the receiver.
    // `sender` is for the instance in which we are the sender.
    //
    // Even though usually only one of these will be filled with something meaningful (not `NONE`),
    // we always send both just to keep the layout of the frame fixed. It simplifies the serialization.
    one_side receiver;
    one_side sender;

    constexpr static size_t serialized_size = 2 * one_side::serialized_size;
    void serialize(std::span<std::byte, serialized_size>);
    static control_protocol_frame deserialize(std::span<const std::byte, serialized_size>);
};

} // namespace utils
