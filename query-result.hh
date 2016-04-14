/*
 * Copyright (C) 2015 ScyllaDB
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
#include "md5_hasher.hh"
#include <experimental/optional>

namespace stdx = std::experimental;

namespace query {

enum class result_request {
    only_result,
    only_digest,
    result_and_digest,
};

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
// Users which need more complex structure of query results can convert this
// to query::result_set.
//
// Related headers:
//  - query-result-reader.hh
//  - query-result-writer.hh


class result {
    bytes_ostream _w;
    stdx::optional<result_digest> _digest;
public:
    class builder;
    class partition_writer;
    friend class result_merger;

    result();
    result(bytes_ostream&& w) : _w(std::move(w)) {}
    result(bytes_ostream&& w, stdx::optional<result_digest> d) : _w(std::move(w)), _digest(d) {}
    result(result&&) = default;
    result(const result&) = default;
    result& operator=(result&&) = default;
    result& operator=(const result&) = default;

    const bytes_ostream& buf() const {
        return _w;
    }

    const stdx::optional<result_digest>& digest() const {
        return _digest;
    }

    struct printer {
        schema_ptr s;
        const query::partition_slice& slice;
        const query::result& res;
    };

    sstring pretty_print(schema_ptr, const query::partition_slice&) const;
    printer pretty_printer(schema_ptr, const query::partition_slice&) const;
};

std::ostream& operator<<(std::ostream& os, const query::result::printer&);
}
