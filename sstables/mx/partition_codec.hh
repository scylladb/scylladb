/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <vector>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "bytes_ostream.hh"
#include "encoding_stats.hh"
#include "mutation/mutation.hh"
#include "reader_permit.hh"
#include "schema/schema_fwd.hh"
#include "sstables/version.hh"

class column_mapping;

// Encodes one partition in the mx data-file format, the bytes an sstable's
// Data.db holds for it, and decodes it back without an sstable.
namespace sstables::mx {

struct codec_params {
    sstable_version_types version;
};

// The column lists the encoder and the decoder agree on, for one writer schema
// and one reader schema. Built once per schema version and cached by the caller.
// Holds no delta-encoding bases: those are passed per call.
class codec_schema {
public:
    struct impl;
    // Defined out of line, where impl is complete.
    explicit codec_schema(lw_shared_ptr<const impl> impl);
    codec_schema(const codec_schema&);
    codec_schema(codec_schema&&) noexcept;
    codec_schema& operator=(const codec_schema&);
    codec_schema& operator=(codec_schema&&) noexcept;
    ~codec_schema();
    const impl& get() const;
    // The fixed lengths of the writer's clustering key types, one per clustering column.
    const std::vector<std::optional<uint32_t>>& clustering_key_fixed_lengths() const;
private:
    lw_shared_ptr<const impl> _impl;
};

// Encodes with writer_schema and decodes with reader_schema; both may be the same.
// Counter and compact tables are refused with on_internal_error().
codec_schema make_codec_schema(const schema& writer_schema, const schema& reader_schema, codec_params params);

// Decodes data written with an older schema, known only by its column mapping and
// the fixed lengths of its clustering key types, one per clustering column. The
// result cannot encode.
codec_schema make_codec_schema(const column_mapping& writer_columns,
        const std::vector<std::optional<uint32_t>>& ck_fixed_lengths,
        const schema& reader_schema, codec_params params);

// Appends the encoding of m to out.
void encode_partition(const mutation& m, const encoding_stats& bases, const codec_schema& cs, bytes_ostream& out);

// in holds the output of encode_partition(); bases must equal the ones it was encoded with.
// Throws malformed_sstable_exception on corrupt input and column_mismatch_error
// (partition_decoder.hh) when the input holds data s cannot take.
future<mutation> decode_partition(schema_ptr s, const encoding_stats& bases, const codec_schema& cs,
        bytes_ostream in, reader_permit permit);

} // namespace sstables::mx
