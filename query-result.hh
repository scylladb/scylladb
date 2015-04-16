/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "bytes_ostream.hh"

namespace query {

//
// The query results are stored in a serialized from. This is in order to
// address the following problems, which a structured format has:
//
//   - high level of indirection (vector of vectors of vectors of blobs), which
//     is not CPU cache friendly
//
//   - high allocation rate due to fine-grained object structure
//
// On replica side, the query results are probably going to be serialized in
// the transport layer anyway, so serializing the results up-front doesn't add
// net work. There is no processing of the query results on replica other than
// concatenation in case of range queries and checksum calculation. If query
// results are collected in serialized form from different cores, we can
// concatenate them without copying by simply appending the fragments into the
// packet.
//
// On coordinator side, the query results would have to be parsed from the
// transport layer buffers anyway, so the fact that iterators parse it also
// doesn't add net work, but again saves allocations and copying. The CQL
// server doesn't need complex data structures to process the results, it just
// goes over it linearly consuming it.
//
// The coordinator side could be optimized even further for CQL queries which
// do not need processing (eg. select * from cf where ...). We could make the
// replica send the query results in the format which is expected by the CQL
// binary protocol client. So in the typical case the coordinator would just
// pass the data using zero-copy to the client, prepending a header.
//
// Users which need more complex structure of query results, should
// trasnform it to such using appropriate visitors.
// TODO: insert reference to such visitors here.
//
// Query results have dynamic format. In some queries (maybe even in typical
// ones), we don't need to send partition or clustering keys back to the
// client, because they are already specified in the query request, and not
// queried for. The query results hold keys optionally.
//
// Also, meta-data like cell timestamp and ttl is optional. It is only needed
// if the query has writetime() or ttl() functions in it, which it typically
// won't have.
//
// Related headers:
//  - query-result-reader.hh
//  - query-result-writer.hh

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
