/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Verification of every checksum of a Parquet file (PLAN.md, "Scrub and
// validation": "Checksum validation verifies the file digest and the
// checksum of every page").
//
// The verifier reads the whole file once, in order, and checks:
//
// - The magic bytes at the start.
// - Every page: its header passes the checks that the reader makes on it
//   (page_decoder.hh), and its CRC matches. The pages of the in-place
//   chunks are located by the OffsetIndexes. That covers the dictionary
//   pages, and the chunks that are entirely null, which the reader never
//   reads.
// - Every OffsetIndex block: it decodes, and its pages tile the row
//   group's region exactly. Each page header agrees with the OffsetIndex on
//   the rows of the page.
// - The page headers of each row group, the late pages included, add up
//   to the totals that the footer records for the row group's chunks: the
//   uncompressed size, the entries, and the nulls. This is checked once
//   the footer has passed its checksum. It needs 32 bytes per row group.
// - The late chunks at the end of the file (see writer.hh), which no
//   OffsetIndex locates and the reader never reads. The writer places them
//   in schema order and then in row group order, adjacent, so the verifier
//   walks them by their page headers. Each must be entirely null and cover
//   the rows of its row group.
// - The checksum of the footer, against the one that the caller expects.
// - The digest of the whole file, if the caller has one.
//
// Page headers carry no checksum. The checks above catch damage to most of
// their fields. A few fields can change to another valid value that no
// check catches: the encoding of a page, the split between the definition
// levels and the repetition levels, and the uncompressed size of a page
// together with another page that changes it back. Only the digest
// detects such damage, so it must be checked too where it is known.
//
// It decodes no values. It reports each problem and goes on where it can:
// a page with a bad checksum or header does not stop the walk, because the
// OffsetIndex locates the next one. A row group whose OffsetIndex block
// does not decode, and late chunks whose header does not parse, are skipped
// up to the next point that the footer locates. The digest is always
// computed over the whole file.

#include <cstdint>
#include <functional>
#include <optional>

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include "reader_permit.hh"
#include "sstables/parquet/file_summary.hh"

namespace sstables::parquet {

struct verify_options {
    /// The size of the file.
    uint64_t file_size;
    /// The CRC-32 of the serialized footer (footer_info::crc32).
    uint32_t footer_crc32;
    /// The CRC-32 of the whole file, if known.
    std::optional<uint32_t> digest;
    /// Stops the walk early when an abort is requested.
    const abort_source* abort = nullptr;
    /// The permit of the walk. The verifier charges to it the buffers that
    /// it allocates and the OffsetIndex blocks that it decodes. It asks
    /// for the memory before it allocates, and waits for it.
    std::optional<reader_permit> permit;
};

/// Verifies the file whose summary is `s`. `in` must deliver the whole file
/// from offset 0. `f` must be the same file. It serves the reads of the
/// OffsetIndex blocks, which precede their pages in the walk but follow them
/// in the file. These reads are aligned to the file's read alignment, and
/// are at most 128 KiB, so a file tracked by the permit charges the size
/// that it allocates. Each problem is passed to `on_error`. Returns the
/// number of problems. An I/O error raises an exception.
future<uint64_t> verify_file(input_stream<char>& in, file f, const file_summary& s, verify_options options,
        std::function<void(sstring)> on_error);

} // namespace sstables::parquet
