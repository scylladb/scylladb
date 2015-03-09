/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/ragel.hh"
#include <memory>
#include <unordered_map>
#include "http/request.hh"

using namespace httpd;

%% machine request;

%%{

access _fsm_;

action mark {
    g.mark_start(p);
}

action store_method {
    _req->_method = str();
}

action store_uri {
    _req->_url = str();
}

action store_version {
    _req->_version = str();
}

action store_field_name {
    _field_name = str();
}

action store_value {
    _value = str();
}

action assign_field {
    _req->_headers[_field_name] = std::move(_value);
}

action extend_field  {
    _req->_headers[_field_name] += sstring(" ") + std::move(_value);
}

action done {
    done = true;
    fbreak;
}

crlf = '\r\n';
tchar = alpha | digit | '-' | '!' | '#' | '$' | '%' | '&' | '\'' | '*'
        | '+' | '.' | '^' | '_' | '`' | '|' | '~';

sp = ' ';
ht = '\t';

sp_ht = sp | ht;

op_char = upper;

operation = op_char+ >mark %store_method;
uri = (any - sp)+ >mark %store_uri;
http_version = 'HTTP/' (digit '.' digit) >mark %store_version;

field = tchar+ >mark %store_field_name;
value = any* >mark %store_value;
start_line = ((operation sp uri sp http_version) -- crlf) crlf;
header_1st = (field sp_ht* ':' value :> crlf) %assign_field;
header_cont = (sp_ht+ value sp_ht* crlf) %extend_field;
header = header_1st header_cont*;
main := start_line header* :> (crlf @done);

}%%

class http_request_parser : public ragel_parser_base<http_request_parser> {
    %% write data nofinal noprefix;
public:
    enum class state {
        error,
        eof,
        done,
    };
    std::unique_ptr<httpd::request> _req;
    sstring _field_name;
    sstring _value;
    state _state;
public:
    void init() {
        init_base();
        _req.reset(new httpd::request());
        _state = state::eof;
        %% write init;
    }
    char* parse(char* p, char* pe, char* eof) {
        sstring_builder::guard g(_builder, p, pe);
        auto str = [this, &g, &p] { g.mark_end(p); return get_str(); };
        bool done = false;
        if (p != pe) {
            _state = state::error;
        }
        %% write exec;
        if (!done) {
            p = nullptr;
        } else {
            _state = state::done;
        }
        return p;
    }
    auto get_parsed_request() {
        return std::move(_req);
    }
    bool eof() const {
        return _state == state::eof;
    }
};
