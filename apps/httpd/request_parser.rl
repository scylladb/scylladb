/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "core/ragel.hh"
#include <memory>
#include <unordered_map>

struct http_request {
    sstring _method;
    sstring _url;
    sstring _version;
    std::unordered_map<sstring, sstring> _headers;
};

%% machine http_request;

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
    std::unique_ptr<http_request> _req;
    sstring _field_name;
    sstring _value;
    state _state;
public:
    void init() {
        _req.reset(new http_request());
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
