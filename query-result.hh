/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "bytes_ostream.hh"

namespace query {

//
// Query results are serialized to the following form:
//
// <result>          ::= <partition>*
// <partition>       ::= <row-count> [ <partition-key> ] [ <static-row> ] <row>*
// <static-row>      ::= <row>
// <row>             ::= <row-length> <cell>+
// <cell>            ::= <atomic-cell> | <collection-cell>
// <atomic-cell>     ::= <present-byte> [ <timestamp> <ttl> ] <value>
// <collection-cell> ::= <blob>
//
// <value>           ::= <blob>
// <blob>            ::= <blob-length> <uint8_t>*
// <timestamp>       ::= <uint64_t>
// <ttl>             ::= <int32_t>
// <present-byte>    ::= <int8_t>
// <row-length>      ::= <uint32_t>
// <row-count>       ::= <uint32_t>
// <blob-length>     ::= <uint32_t>
//
// Optional elements are present according to partition_slice
//
class result {
    bytes_ostream _w;
public:
    class builder;
    class partition_writer;
    class row_writer;
    friend class result_merger;

    result() {}
    result(bytes_ostream&& w) : _w(std::move(w)) {}

    const bytes_ostream& buf() const {
        return _w;
    }
};

}
