/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "server.hh"

namespace cql_transport {

enum class cql_binary_opcode : uint8_t {
    ERROR          = 0,
    STARTUP        = 1,
    READY          = 2,
    AUTHENTICATE   = 3,
    CREDENTIALS    = 4,
    OPTIONS        = 5,
    SUPPORTED      = 6,
    QUERY          = 7,
    RESULT         = 8,
    PREPARE        = 9,
    EXECUTE        = 10,
    REGISTER       = 11,
    EVENT          = 12,
    BATCH          = 13,
    AUTH_CHALLENGE = 14,
    AUTH_RESPONSE  = 15,
    AUTH_SUCCESS   = 16,
    OPCODES_COUNT
};

class response {
    int16_t           _stream;
    cql_binary_opcode _opcode;
    uint8_t           _flags = 0; // a bitwise OR mask of zero or more cql_frame_flags values
    bytes_ostream _body;
public:
    template<typename T>
    class placeholder;

    response(int16_t stream, cql_binary_opcode opcode, const tracing::trace_state_ptr& tr_state_ptr)
        : _stream{stream}
        , _opcode{opcode}
    {
        if (tracing::should_return_id_in_response(tr_state_ptr)) {
            auto i = _body.write_place_holder(utils::UUID::serialized_size());
            tr_state_ptr->session_id().serialize(i);
            set_frame_flag(cql_frame_flags::tracing);
        }
    }

    void set_frame_flag(cql_frame_flags flag) noexcept {
        _flags |= flag;
    }

    void serialize(const event::schema_change& event, uint8_t version);
    void write_byte(uint8_t b);
    void write_int(int32_t n);
    placeholder<int32_t> write_int_placeholder();
    void write_long(int64_t n);
    void write_short(uint16_t n);
    void write_string(std::string_view s);
    void write_bytes_as_string(bytes_view s);
    void write_long_string(const sstring& s);
    void write_string_list(std::vector<sstring> string_list);
    void write_bytes(bytes b);
    void write_short_bytes(bytes b);
    void write_inet(socket_address inet);
    void write_consistency(db::consistency_level c);
    void write_string_map(std::map<sstring, sstring> string_map);
    void write_string_multimap(std::multimap<sstring, sstring> string_map);
    void write_string_bytes_map(const std::unordered_map<sstring, bytes>& map);
    void write_value(bytes_opt value);
    void write_value(std::optional<managed_bytes_view> value);
    void write(const cql3::metadata& m, bool skip = false);
    void write(const cql3::prepared_metadata& m, uint8_t version);

    // Make a non-owning scattered_message of the response. Remains valid as long
    // as the response object is alive.
    scattered_message<char> make_message(uint8_t version, cql_compression compression);

    cql_binary_opcode opcode() const {
        return _opcode;
    }
    size_t size() const {
        return _body.size();
    }
private:
    void compress(cql_compression compression);
    void compress_lz4();
    void compress_snappy();

    template <typename CqlFrameHeaderType>
    sstring make_frame_one(uint8_t version, size_t length) {
        sstring frame_buf = uninitialized_string(sizeof(CqlFrameHeaderType));
        auto* frame = reinterpret_cast<CqlFrameHeaderType*>(frame_buf.data());
        frame->version = version | 0x80;
        frame->flags   = _flags;
        frame->opcode  = static_cast<uint8_t>(_opcode);
        frame->length  = htonl(length);
        frame->stream = net::hton((decltype(frame->stream))_stream);

        return frame_buf;
    }

    sstring make_frame(uint8_t version, size_t length) {
        if (version > 0x04) {
            throw exceptions::protocol_exception(format("Invalid or unsupported protocol version: {:d}", version));
        }

        return make_frame_one<cql_binary_frame_v3>(version, length);
    }
};

template<>
class response::placeholder<int32_t> {
    int8_t* _pointer;
public:
    explicit placeholder(int8_t* ptr) : _pointer(ptr) { }
    void write(int32_t n) {
        auto u = htonl(n);
        auto* s = reinterpret_cast<const int8_t*>(&u);
        std::copy_n(s, sizeof(u), _pointer);
    }
};

}
