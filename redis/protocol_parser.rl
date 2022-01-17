/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <memory>
#include <iostream>
#include <algorithm>
#include <functional>
#include "bytes.hh"
#include "redis/request.hh"
#include <seastar/core/ragel.hh>

using namespace seastar;
using namespace redis;
%%{

machine redis_resp_protocol;

access _fsm_;

action mark {
    g.mark_start(p);
}

action start_blob {
    g.mark_start(p);
    _bytes_left = _bytes_count;
}

action start_command {
    g.mark_start(p);
    _bytes_left = _bytes_count;
}

action advance_blob {
    auto len = std::min(static_cast<uint32_t>(pe - p), _bytes_left);
    _bytes_left -= len;
    p += len;
    if (_bytes_left == 0) {
      _req._args.push_back(str());
      p--;
      fret;
    }
    p--;
}

action advance_command {
    auto len = std::min(static_cast<uint32_t>(pe - p), _bytes_left);
    _bytes_left -= len;
    p += len;
    if (_bytes_left == 0) {
      _req._command = str();
      p--;
      fret;
    }
    p--;
}


crlf = '\r\n';
u32 = digit+ >{ _u32 = 0;}  ${ _u32 *= 10; _u32 += fc - '0';};
args_count = '*' u32 crlf ${_req._args_count = _u32 - 1;};
blob := any+ >start_blob $advance_blob;
command := any+ >start_command $advance_command;
arg = '$' u32 crlf ${ _bytes_count = _u32;};

main := (args_count (arg @{fcall command; } crlf) (arg @{fcall blob; } crlf)+) ${_req._state = request_state::ok;} >eof{_req._state = request_state::eof;};

prepush {
    prepush();
}

postpop {
    postpop();
}

}%%

class redis_protocol_parser : public ragel_parser_base<redis_protocol_parser> {
    %% write data nofinal noprefix;
public:
    redis::request _req;
    uint32_t _u32;
    uint32_t _bytes_count;
    uint32_t _bytes_left;
public:
    virtual void init() {
        init_base();
        _req._state = request_state::error;
        _req._args.clear();
        _req._args_count = 0;
        _bytes_left = 0;
        _bytes_count = 0;
        %% write init;
    }

    char* parse(char* p, char* pe, char* eof) {
        sstring_builder::guard g(_builder, p, pe);
        auto str = [this, &g, &p] {
            g.mark_end(p);
            auto s =  get_str();
            return to_bytes(s);
        };

        %% write exec;
        // does not reach to the tail of the message, continue reading
        if (_bytes_left || _req._args_count - _req._args.size()) {
            return nullptr;
        }

        if (_req._state != request_state::error) {
            return p;
        }
        if (p != pe) {
            p = pe;
            return p;
        }
        return nullptr;
    }
    
    bool eof() const {
        return _req._state == request_state::eof;
    }
    
    bool failed() const {
        return _req._state == request_state::error;
    }

    redis::request& get_request() { 
        std::transform(_req._command.begin(), _req._command.end(), _req._command.begin(), ::tolower);
        return _req;
    }
};
