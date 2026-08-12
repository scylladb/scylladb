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

// The on-disk header types below all have a fixed serialized size. Their encoding lives in the
// ser::serializer<> specializations at the bottom of this file, where each specialization declares
// its own serialized_size next to the write()/read()/skip() that produce and consume those bytes.
// The ondisk::*_size constants are aliases of those, so there is a single source of truth.
//
// serialized_size must stay in sync with write()/read()/skip(): it is what sizes the substreams in
// write_buffer and the reads in segment_io, so a mismatch silently truncates or pads records on disk
// instead of failing. Nothing enforces this at compile time - the serializers bottom out in
// reinterpret_cast, which cannot be evaluated in a constant expression - so it is verified at runtime
// by test_logstor_ondisk_serialized_sizes in test/boost/logstor_test.cc.
//
// When adding a field to one of these types, or adding a new fixed-size on-disk type: update or write
// all four of write(), read(), skip() and serialized_size together, add an alias in the ondisk block
// at the bottom of the file if the size is needed elsewhere, and add a check_serialized_size() call
// for the type to that test with a value whose fields are all distinct and non-zero.

struct buffer_header {
    uint32_t magic;
    segment_kind kind;
    uint8_t version;
    uint16_t reserved;
    segment_sequence segment_seq;
    uint32_t data_size; // size of all records data following the header(s)
    uint32_t crc;

    uint32_t calculate_crc() const;

    bool operator==(const buffer_header& other) const noexcept = default;
};

struct segment_header {
    table_id table;
    dht::token first_token;
    dht::token last_token;

    bool operator==(const segment_header& other) const noexcept = default;
};

struct record_header {
    uint32_t data_size; // size of the serialized canonical_mutation

    bool operator==(const record_header& other) const noexcept = default;
};

bool validate_header(const buffer_header& bh);
bool validate_record_header(const record_header& rh);

} // namespace ondisk
} // namespace replica::logstor

namespace ser {

template <>
struct serializer<replica::logstor::primary_index_key> {
    static constexpr size_t serialized_size =
        sizeof(int64_t)             // token
        + replica::logstor::key_hash_size;

    template <typename Output>
    static void write(Output& out, const replica::logstor::primary_index_key& key) {
        serializer<int64_t>::write(out, key._token.raw());
        out.write(reinterpret_cast<const char*>(key._hash.data()), key._hash.size());
    }
    template <typename Input>
    static replica::logstor::primary_index_key read(Input& in) {
        replica::logstor::primary_index_key key;
        key._token = dht::token::from_int64(serializer<int64_t>::read(in));
        in.read(reinterpret_cast<char*>(key._hash.data()), key._hash.size());
        return key;
    }
    template <typename Input>
    static void skip(Input& in) {
        serializer<int64_t>::skip(in);
        in.skip(replica::logstor::key_hash_size);
    }
};

template <>
struct serializer<replica::logstor::log_record_header> {
    static constexpr size_t serialized_size =
        serializer<replica::logstor::primary_index_key>::serialized_size
        + sizeof(api::timestamp_type)
        + 2 * sizeof(int64_t);      // table id

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
    static constexpr size_t serialized_size =
        sizeof(uint32_t)            // magic
        + sizeof(uint8_t)           // kind
        + sizeof(uint8_t)           // version
        + sizeof(uint16_t)          // reserved
        + sizeof(uint64_t)          // segment_seq
        + sizeof(uint32_t)          // data_size
        + sizeof(uint32_t);         // crc

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
    static constexpr size_t serialized_size =
        2 * sizeof(int64_t)         // table id, written by serializer<table_id> as two int64
        + 2 * sizeof(int64_t);      // first and last token

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
    static constexpr size_t serialized_size = sizeof(uint32_t);   // data_size

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

namespace replica::logstor::ondisk {

// The on-disk sizes are aliases of the serializers above, so that each size is stated
// next to the code that writes it.
static constexpr size_t serialized_primary_index_key_size = ser::serializer<primary_index_key>::serialized_size;
static constexpr size_t serialized_log_record_header_size = ser::serializer<log_record_header>::serialized_size;
static constexpr size_t buffer_header_size = ser::serializer<buffer_header>::serialized_size;
static constexpr size_t segment_header_size = ser::serializer<segment_header>::serialized_size;
static constexpr size_t record_header_size = ser::serializer<record_header>::serialized_size;

static_assert(buffer_header_size % record_alignment == 0, "Buffer header size must be aligned by record_alignment");
static_assert(segment_header_size % record_alignment == 0, "Segment header size must be aligned by record_alignment");

} // namespace replica::logstor::ondisk
