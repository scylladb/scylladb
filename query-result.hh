/*
 * Copyright 2015 Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include <cryptopp/md5.h>
#include "bytes_ostream.hh"
#include "query-request.hh"
#include "db/serializer.hh"

namespace query {

class result_digest {
public:
    static_assert(16 == CryptoPP::Weak::MD5::DIGESTSIZE, "MD5 digest size is all wrong");
    using type = std::array<uint8_t, 16>;
private:
    type _digest;
public:
    result_digest() = default;
    result_digest(type&& digest) : _digest(std::move(digest)) {}
    const type& get() const { return _digest; }
    bool operator==(const result_digest& rh) const {
        return _digest == rh._digest;
    }
    bool operator!=(const result_digest& rh) const {
        return _digest != rh._digest;
    }
};

//
// The query results are stored in a serialized form. This is in order to
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
// transform it to such using appropriate visitors.
// TODO: insert reference to such visitors here.
//
// Query results have dynamic format. In some queries (maybe even in typical
// ones), we don't need to send partition or clustering keys back to the
// client, because they are already specified in the query request, and not
// queried for. The query results hold keys optionally.
//
// Also, meta-data like cell timestamp and expiry is optional. It is only needed
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
// <atomic-cell>     ::= <present-byte> [ <timestamp> <expiry> ] <value>
// <collection-cell> ::= <blob>
//
// <value>           ::= <blob>
// <blob>            ::= <blob-length> <uint8_t>*
// <timestamp>       ::= <uint64_t>
// <expiry>          ::= <int32_t>
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
    result(result&&) = default;
    result(const result&) = default;
    result& operator=(result&&) = default;
    result& operator=(const result&) = default;

    const bytes_ostream& buf() const {
        return _w;
    }

    result_digest digest() {
        CryptoPP::Weak::MD5 hash;
        result_digest::type digest;
        bytes_view v = _w.linearize();
        hash.CalculateDigest(reinterpret_cast<unsigned char*>(digest.data()), reinterpret_cast<const unsigned char*>(v.begin()), v.size());
        return result_digest(std::move(digest));
    }
    sstring pretty_print(schema_ptr, const query::partition_slice&) const;
};

}
