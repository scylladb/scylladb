/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/types.hh"
#include "schema/schema_fwd.hh"

class mutation;

namespace replica::logstor {

// The two functions that know what the bytes of a record_value mean. Everything else in logstor
// treats a value as bytes, so changing the encoding is a change to these two alone.
//
// The value holds the partition of the mutation and nothing the record_header already
// carries, which is why decoding takes the header: the key and the table of the record come
// from it. The layout of the value is described in replica/logstor/record_value.cc, next to
// the encoder.

// Encodes the partition of m as the value of a record whose header is built from m.
record_value encode_record_value(const mutation& m);

// Decodes the value of a record with header h as a mutation of the schema s.
mutation decode_record_value(const record_value& v, schema_ptr s, const record_header& h);

// The mutation a whole record holds.
mutation to_mutation(const log_record& r, schema_ptr s);

} // namespace replica::logstor
