/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "core/ragel.hh"

#include <algorithm>

/*
 * CQL binary protocol versions [1234]
 */

%%{

machine cql_binary_protocol;

access _fsm_;

action mark {
    _g = p;
}

v1 = 0x01 >mark @{ _version = read_u8(); };

v2 = 0x02 >mark @{ _version = read_u8(); };

v3 = 0x03 >mark @{ _version = read_u8(); };

v4 = 0x04 >mark @{ _version = read_u8(); };

v1_v2 = v1|v2;

v3_v4 = v3|v4;

flags = any{1} >mark @{ _flags = read_u8(); };

stream8 = any{1} >mark @{ _stream = read_u8(); };

stream16 = any{2} >mark @{ _stream = read_be16(); };

length = any{4} >mark @{ _length = read_be32(); };

startup_v1_v2 = v1_v2 flags stream8 0x01 length @{ _state = state::req_startup; };

credentials_v1 = v1 flags stream8 0x04 length @{ _state = state::req_credentials; };

auth_response_v2 = v2 flags stream8 0x0f length @{ _state = state::req_auth_response; };

options_v1_v2 = v1_v2 flags stream8 0x05 length @{ _state = state::req_options; };

query_v1_v2 = v1_v2 flags stream8 0x07 length @{ _state = state::req_query; };

prepare_v1_v2 = v1_v2 flags stream8 0x09 length @{ _state = state::req_prepare; };

execute_v1_v2 = v1_v2 flags stream8 0x0a length @{ _state = state::req_execute; };

batch_v1_v2 = v1_v2 flags stream8 0x0d length @{ _state = state::req_batch; };

register_v1_v2 = v1_v2 flags stream8 0x0b length @{ _state = state::req_register; };

request_v1_v2 = startup_v1_v2|credentials_v1|auth_response_v2|options_v1_v2|query_v1_v2|prepare_v1_v2|execute_v1_v2|batch_v1_v2|register_v1_v2;

startup_v3_v4 = v3_v4 flags stream16 0x01 length @{ _state = state::req_startup; };

auth_response_v3_v4 = v3_v4 flags stream16 0x0f length @{ _state = state::req_auth_response; };

options_v3_v4 = v3_v4 flags stream16 0x05 length @{ _state = state::req_options; };

query_v3_v4 = v3_v4 flags stream16 0x07 length @{ _state = state::req_query; };

prepare_v3_v4 = v3_v4 flags stream16 0x09 length @{ _state = state::req_prepare; };

execute_v3_v4 = v3_v4 flags stream16 0x0a length @{ _state = state::req_execute; };

batch_v3_v4 = v3_v4 flags stream16 0x0d length @{ _state = state::req_batch; };

register_v3_v4 = v3_v4 flags stream16 0x0b length @{ _state = state::req_register; };

request_v3_v4 = startup_v3_v4|auth_response_v3_v4|options_v3_v4|query_v3_v4|prepare_v3_v4|execute_v3_v4|batch_v3_v4|register_v3_v4;

main := request_v1_v2|request_v3_v4 @eof{ _state = state::eof; };

prepush {
    prepush();
}

postpop {
    postpop();
}

}%%

class cql_binary_parser : public ragel_parser_base<cql_binary_parser> {
    %% write data nofinal noprefix;
public:
    enum class state {
        error,
        eof,
        req_startup,
        req_credentials,
        req_auth_response,
        req_options,
        req_query,
        req_prepare,
        req_execute,
        req_batch,
        req_register,
    };
    state   _state;
    int8_t  _version;
    int8_t  _flags;
    int16_t _stream;
    int32_t _length;
    char*   _g;
public:
    void init() {
        init_base();
        _state = state::error;
        %% write init;
    }
    char* parse(char* p, char* pe, char* eof) {
        %% write exec;
        if (_state != state::error) {
            return p;
        }
        if (p != pe) {
            p = pe;
            return p;
        }
        return nullptr;
    }
    uint8_t read_u8() {
        return _g[0];
    }
    uint16_t read_be16() {
        return (static_cast<uint8_t>(_g[0]) << 8)
             | (static_cast<uint8_t>(_g[1]));
    }
    uint32_t read_be32() {
        return (static_cast<uint8_t>(_g[0]) << 24)
             | (static_cast<uint8_t>(_g[1]) << 16)
             | (static_cast<uint8_t>(_g[2]) <<  8)
             | (static_cast<uint8_t>(_g[3]));
    }
    bool eof() const {
        return _state == state::eof;
    }
};
