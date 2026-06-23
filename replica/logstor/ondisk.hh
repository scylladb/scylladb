/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "idl/uuid.dist.hh"
#include "idl/uuid.dist.impl.hh"
#include "dht/token.hh"
#include "replica/logstor/types.hh"
#include "serializer.hh"

namespace replica::logstor {

namespace ondisk {

static constexpr size_t block_alignment = 4096;
static constexpr size_t record_alignment = 8;
static constexpr uint8_t current_version = 1;
static constexpr uint32_t buffer_header_magic = 0x4c475342;

static constexpr size_t serialized_primary_index_key_size = key_hash_size;
static constexpr size_t serialized_log_record_header_size = serialized_primary_index_key_size + sizeof(api::timestamp_type) + utils::UUID::serialized_size();

struct buffer_header {
    uint32_t magic;
    segment_kind kind;
    uint8_t version;
    uint16_t reserved;
    segment_sequence segment_seq;
    uint32_t data_size; // size of all records data following the header(s)
    uint32_t crc;

    uint32_t calculate_crc() const;
};

static constexpr size_t buffer_header_size =
    sizeof(uint32_t)
    + sizeof(std::underlying_type_t<segment_kind>)
    + sizeof(uint8_t)
    + sizeof(uint16_t)
    + sizeof(segment_sequence)
    + sizeof(uint32_t)
    + sizeof(uint32_t);
static_assert(buffer_header_size % record_alignment == 0, "Buffer header size must be aligned by record_alignment");

struct segment_header {
    table_id table;
    dht::token first_token;
    dht::token last_token;
};

static constexpr size_t segment_header_size =
    sizeof(table_id)
    + 2 * sizeof(int64_t);
static_assert(segment_header_size % record_alignment == 0, "Segment header size must be aligned by record_alignment");

struct record_header {
    uint32_t data_size; // size of the serialized canonical_mutation
};

static constexpr size_t record_header_size = sizeof(uint32_t);

bool validate_header(const buffer_header& bh);
bool validate_record_header(const record_header& rh);

} // namespace ondisk
} // namespace replica::logstor

namespace ser {

template <>
struct serializer<replica::logstor::primary_index_key> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::primary_index_key& key) {
        auto h = key.hash();
        out.write(reinterpret_cast<const char*>(h.data()), h.size());
    }
    template <typename Input>
    static replica::logstor::primary_index_key read(Input& in) {
        replica::logstor::key_hash hash;
        in.read(reinterpret_cast<char*>(hash.data()), hash.size());
        return replica::logstor::primary_index_key(hash);
    }
    template <typename Input>
    static void skip(Input& in) {
        in.skip(replica::logstor::key_hash_size);
    }
};

template <>
struct serializer<replica::logstor::log_record_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::log_record_header& h) {
        serializer<replica::logstor::primary_index_key>::write(out, h.key);
        serializer<api::timestamp_type>::write(out, h.timestamp);
        serializer<int64_t>::write(out, h.table.uuid().get_most_significant_bits());
        serializer<int64_t>::write(out, h.table.uuid().get_least_significant_bits());
    }
    template <typename Input>
    static replica::logstor::log_record_header read(Input& in) {
        replica::logstor::log_record_header h;
        h.key = serializer<replica::logstor::primary_index_key>::read(in);
        h.timestamp = serializer<api::timestamp_type>::read(in);
        auto msb = serializer<int64_t>::read(in);
        auto lsb = serializer<int64_t>::read(in);
        h.table = table_id(utils::UUID(msb, lsb));
        return h;
    }
    template <typename Input>
    static void skip(Input& in) {
        serializer<replica::logstor::primary_index_key>::skip(in);
        serializer<api::timestamp_type>::skip(in);
        serializer<int64_t>::skip(in);
        serializer<int64_t>::skip(in);
    }
};

template <>
struct serializer<replica::logstor::ondisk::buffer_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::ondisk::buffer_header& h) {
        serializer<uint32_t>::write(out, h.magic);
        serializer<uint8_t>::write(out, static_cast<uint8_t>(h.kind));
        serializer<uint8_t>::write(out, h.version);
        serializer<uint16_t>::write(out, h.reserved);
        serializer<uint64_t>::write(out, h.segment_seq.value);
        serializer<uint32_t>::write(out, h.data_size);
        serializer<uint32_t>::write(out, h.crc);
    }

    template <typename Input>
    static replica::logstor::ondisk::buffer_header read(Input& in) {
        replica::logstor::ondisk::buffer_header h;
        h.magic = serializer<uint32_t>::read(in);
        h.kind = static_cast<replica::logstor::segment_kind>(serializer<uint8_t>::read(in));
        h.version = serializer<uint8_t>::read(in);
        h.reserved = serializer<uint16_t>::read(in);
        h.segment_seq = replica::logstor::segment_sequence{serializer<uint64_t>::read(in)};
        h.data_size = serializer<uint32_t>::read(in);
        h.crc = serializer<uint32_t>::read(in);
        return h;
    }

    template <typename Input>
    static void skip(Input& in) {
        serializer<uint32_t>::skip(in);
        serializer<uint8_t>::skip(in);
        serializer<uint8_t>::skip(in);
        serializer<uint16_t>::skip(in);
        serializer<uint64_t>::skip(in);
        serializer<uint32_t>::skip(in);
        serializer<uint32_t>::skip(in);
    }
};

template <>
struct serializer<replica::logstor::ondisk::segment_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::ondisk::segment_header& h) {
        serializer<table_id>::write(out, h.table);
        serializer<int64_t>::write(out, h.first_token.raw());
        serializer<int64_t>::write(out, h.last_token.raw());
    }

    template <typename Input>
    static replica::logstor::ondisk::segment_header read(Input& in) {
        replica::logstor::ondisk::segment_header h;
        h.table = serializer<table_id>::read(in);
        h.first_token = dht::token::from_int64(serializer<int64_t>::read(in));
        h.last_token = dht::token::from_int64(serializer<int64_t>::read(in));
        return h;
    }

    template <typename Input>
    static void skip(Input& in) {
        serializer<table_id>::skip(in);
        serializer<int64_t>::skip(in);
        serializer<int64_t>::skip(in);
    }
};

template <>
struct serializer<replica::logstor::ondisk::record_header> {
    template <typename Output>
    static void write(Output& out, const replica::logstor::ondisk::record_header& h) {
        serializer<uint32_t>::write(out, h.data_size);
    }

    template <typename Input>
    static replica::logstor::ondisk::record_header read(Input& in) {
        replica::logstor::ondisk::record_header h;
        h.data_size = serializer<uint32_t>::read(in);
        return h;
    }

    template <typename Input>
    static void skip(Input& in) {
        serializer<uint32_t>::skip(in);
    }
};

} // namespace ser
