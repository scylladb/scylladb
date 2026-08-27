/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Reads a Parquet file image back into the column_data the writer consumes,
// which -- combined with schema_mapping::reassemble -- closes the round trip:
// rows -> Parquet -> rows.
//
// Whole-image for now, mirroring the writer. A seastar-native streaming reader
// is a separate piece of work; the point of this one is that nothing written
// can be trusted until it can be read back and compared.

#include "parquet_metadata.hh"
#include "parquet_writer.hh"

#include <map>
#include <span>
#include <vector>

namespace sstables::parquet::format {

// Everything needed to open an encrypted file: the key, plus the AAD material the file's own
// FileCryptoMetaData declared. Passed as a pointer that defaults to null everywhere below, so an
// unencrypted read is unchanged and an encrypted one is impossible to attempt by accident.
struct read_crypto {
    encryption_key key;                 // the footer key
    cipher         algo = cipher::aes_gcm_v1;
    std::string    aad_prefix;
    std::string    aad_file_unique;
    // Keys for columns that have their own, by leaf name. A reader may legitimately hold some and
    // not others -- that is the point of per-column keys -- so a column with no key here and no
    // footer-key access simply cannot be decoded, and the reader has to say so rather than guess.
    std::map<std::string, encryption_key> column_keys{};

    const encryption_key& key_for(const std::string& leaf) const {
        auto it = column_keys.find(leaf);
        return it == column_keys.end() ? key : it->second;
    }
};

// Open an encrypted-footer ("PARE") file: check the magic, read the plaintext
// FileCryptoMetaData, decrypt and parse the footer. `aad_prefix` is needed only when the writer
// chose not to store it, which the returned crypto records either way.
struct encrypted_footer {
    file_metadata md;
    read_crypto   crypto;
};
encrypted_footer parse_encrypted_footer(std::span<const uint8_t> image, const encryption_key&,
                                        std::string_view aad_prefix = {},
                                        const std::map<std::string, encryption_key>&
                                                column_keys = {},
                                        limits = {});

// Where the time inside a page decode goes. A *breakdown* of the reader's rg_decode and
// decode_cpu phases, not a peer of them: these are nested inside those, so they are reported
// separately and must never be added into the reader's own share column.
//
// It exists because once pq_reader's window was clipped to the rows a point read actually wants,
// every remaining phase of the read except this one was under 10 % -- and "the page decode" is
// four quite different costs wearing one label. Which of them dominates decides whether the lever
// is the codec, the page size or the decode loop, and guessing wrong there has cost a wrong
// answer before (design doc 10.27, 10.28). Same switch as the reader's own profile,
// PQ_READER_PROFILE=1, and likewise always compiled.
enum class dphase : size_t {
    decompress,      // the codec, on a whole data page: zstd cannot decompress part of a frame
    decompress_dict, // the same codec on a *dictionary* page, split out because a dictionary is
                     // shared by every read of its column chunk while a data page is not -- so
                     // this share is what a cache could remove and the one above is not
    levels,         // repetition and definition level streams, RLE-decoded for the whole page
    values,         // the wanted slice of the page's values, plus any skip the encoding forces
    expand_nulls,   // re-expanding a sparse column to one entry per slot
    trim,           // clipping a column to the requested row range
    plan,           // per-call setup: walking the schema tree for leaf level bounds
    _count
};
std::string decode_profile_report();
void decode_profile_reset();

// Decode one row group into one column_data per leaf, in schema order.
std::vector<column_data> read_row_group(std::span<const uint8_t> image,
                                        const file_metadata&,
                                        size_t row_group_index,
                                        const read_crypto* = nullptr);

// The bytes one leaf column needs for a ranged decode. Splitting the dictionary
// page from the data pages is what lets a point read fetch two small extents per
// column instead of the whole row group: the dictionary lives at the head of the
// chunk, the wanted pages live somewhere in the middle, and the pages between
// them never have to be read at all.
struct column_input {
    std::span<const uint8_t> dict;       // dictionary page, header included; may be empty
    std::span<const uint8_t> pages;      // a contiguous run of data pages
    int64_t first_row = 0;               // row index, within the row group, of pages[0]
    // Do not read this leaf: no bytes were fetched for it, and the decoded column_data comes back
    // flagged `skipped` for the reassembler to treat as all-null. Distinct from an empty `pages`
    // span, which is a caller error.
    bool absent = false;
    // Absolute file offset that pages[0] sits at, so a page can be named by where it is in the
    // file rather than by where it landed in this caller's buffer. Only needed to key the page
    // cache; a caller that passes no cache can leave it zero.
    int64_t pages_file_offset = 0;
};

// Decompressed data pages, retained across reads by whoever owns the file.
//
// A page is the unit the codec can inflate -- zstd cannot decompress part of a frame -- and at
// shipping defaults a page is the whole column chunk, so a 5-row point read inflates ~5 000 rows
// per column at ~31 us each. That is ~38 % of a point read (design doc 10.40), and it is the same
// bytes every time: 1 000 random point reads over 20 000 partitions touch 20 row groups, so each
// page is inflated once and then wanted ~50 more times.
//
// Keyed by the absolute file offset of the compressed page body, which identifies a page uniquely
// within a file. The implementation lives next to the sstable that owns the lifetime, so the format
// layer stays free of sstable types.
//
// Contract: a pointer returned by get() or put() stays valid for the rest of the decode. That is
// cheap to honour because decode_columns() is synchronous -- there is no suspension point inside a
// decode -- and because an implementation must not evict during one.
class page_cache {
public:
    virtual ~page_cache() = default;
    virtual const std::vector<uint8_t>* get(int64_t page_body_offset) const noexcept = 0;
    // Asked before put(), so that a decline costs nothing. Without it put() would have to take the
    // page by value and hand it back on refusal, which means copying a page -- cheaper than
    // inflating one, but not by enough to spend on a cache that is full.
    virtual bool accepts(size_t bytes) const noexcept = 0;
    // Only called after accepts() returned true, so it takes ownership and never returns null.
    virtual const std::vector<uint8_t>* put(int64_t page_body_offset, std::vector<uint8_t>) = 0;
};

// Decode rows [row_lo, row_hi) from per-column byte spans. One entry per leaf,
// in schema order.
std::vector<column_data> decode_columns(std::span<const column_input>,
                                        const file_metadata&, size_t row_group_index,
                                        int64_t row_lo, int64_t row_hi,
                                        const read_crypto* = nullptr,
                                        page_cache* = nullptr);

// Decode rows [row_lo, row_hi) of one row group. `base_offset` is the file
// offset that image[0] maps to, so the caller can hand over just the bytes that
// matter instead of the whole file. Pages outside the range are stepped over
// using the V2 header's num_rows without being decompressed -- this is what
// makes a point read cost one page rather than one file.
// `skip`, when non-empty, has one byte per leaf: non-zero means do not decode that leaf, and its
// column_data comes back flagged `skipped`. (Bytes rather than bools because the caller's mask is
// a vector, and std::vector<bool> is not a range of bools.) The bytes are in `image` either way --
// the row group is one sequential read -- so this saves the header walk and the decode, not I/O.
std::vector<column_data> read_row_range(std::span<const uint8_t> image, int64_t base_offset,
                                        const file_metadata&, size_t row_group_index,
                                        int64_t row_lo, int64_t row_hi,
                                        const read_crypto* = nullptr,
                                        std::span<const uint8_t> skip = {});

// Convenience: parse the footer and decode row group 0.
std::vector<column_data> read_file(std::span<const uint8_t> image);

// Convenience for an encrypted file: parse the encrypted footer and decode row group 0.
std::vector<column_data> read_encrypted_file(std::span<const uint8_t> image,
                                             const encryption_key&,
                                             std::string_view aad_prefix = {});

} // namespace sstables::parquet::format
