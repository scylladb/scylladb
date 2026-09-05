/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Measures what a trained zstd dictionary would buy on the Parquet side.
// Answers docs/dev/parquet-storage-format.md section 10.1e / open question 7.
//
// Every ratio in section 10 compares a *dictionary-compressed* SSTable against
// *non*-dictionary Parquet. That is the honest conservative direction -- it
// understates Parquet -- but it leaves the question open: if Parquet used the
// same trick, how much more would it save, and is it worth losing the ability
// for any other Parquet implementation to open the file?
//
// Method. A dictionary acts at the codec layer, below encoding, so this does
// not need to decode a single value: it walks the pages of a real Parquet file,
// decompresses each page body, and recompresses it two ways -- zstd-3 alone and
// zstd-3 with a trained dictionary. Column types, encodings and logical types
// are therefore irrelevant, which is what makes it possible to run this over
// 105- and 197-column real datasets that our value decoders do not fully cover.
//
// Both the self-trained and the held-out numbers are reported. Training on the
// same bytes you then compress is optimistic, and it is also exactly what
// Scylla's own sstable_dict_autotrainer does, so the self-trained column is the
// like-for-like comparison against the baseline. The held-out column trains on
// the first half of the pages and measures the second half, which is the honest
// answer to "what does this do on data the dictionary has not seen".

#include "parquet_metadata.hh"
#include "page_header.hh"

#include <zstd.h>
#include <zdict.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <numeric>
#include <span>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

namespace {

std::vector<uint8_t> slurp(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) { throw std::runtime_error("cannot open " + path); }
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(f)),
                                 std::istreambuf_iterator<char>());
}

std::vector<uint8_t> zstd_decompress(std::span<const uint8_t> in, size_t expect) {
    std::vector<uint8_t> out(expect);
    auto r = ZSTD_decompress(out.data(), out.size(), in.data(), in.size());
    if (ZSTD_isError(r)) { throw std::runtime_error(std::string("zstd: ") + ZSTD_getErrorName(r)); }
    out.resize(r);
    return out;
}

// One page body, already decompressed. `payload_only` is the part a codec sees:
// for V2 that excludes the levels, which are never compressed.
struct page_body {
    std::vector<uint8_t> raw;
};

// Walk every page of every column chunk and hand back the uncompressed bodies.
std::vector<page_body> collect_pages(std::span<const uint8_t> img, const file_metadata& md,
                                     size_t& on_disk_page_bytes, size_t& skipped) {
    std::vector<page_body> out;
    for (const auto& rg : md.row_groups) {
        for (const auto& cc : rg.columns) {
            if (!cc.meta) { continue; }
            const auto& cm = *cc.meta;
            int64_t off = cm.dictionary_page_offset ? *cm.dictionary_page_offset : cm.data_page_offset;
            const int64_t end = off + cm.total_compressed_size;
            if (off < 0 || size_t(end) > img.size()) { continue; }

            while (off < end) {
                size_t consumed = 0;
                page_header ph;
                try {
                    ph = parse_page_header(
                            img.subspan(size_t(off), size_t(std::min<int64_t>(end - off, 64 * 1024))),
                            consumed);
                } catch (...) { break; }

                const int64_t body_at = off + int64_t(consumed);
                if (body_at + ph.compressed_page_size > int64_t(img.size())) { break; }
                auto body = img.subspan(size_t(body_at), size_t(ph.compressed_page_size));
                on_disk_page_bytes += size_t(consumed) + size_t(ph.compressed_page_size);

                // V2 keeps the levels outside the compressed region, so only the
                // tail is codec input. V1 and dictionary pages compress whole.
                size_t lvl = 0;
                bool compressed = true;
                if (ph.type == page_type::data_page_v2 && ph.v2) {
                    lvl = size_t(ph.v2->repetition_levels_byte_length) +
                          size_t(ph.v2->definition_levels_byte_length);
                    compressed = ph.v2->is_compressed;
                }
                if (lvl > body.size()) { break; }
                auto payload = body.subspan(lvl);
                const size_t expect = size_t(ph.uncompressed_page_size) - lvl;

                try {
                    page_body pb;
                    if (compressed && cm.compression == codec::zstd) {
                        pb.raw = zstd_decompress(payload, expect);
                    } else if (!compressed || cm.compression == codec::uncompressed) {
                        pb.raw.assign(payload.begin(), payload.end());
                    } else {
                        ++skipped;   // some other codec; not our measurement
                        off = body_at + ph.compressed_page_size;
                        continue;
                    }
                    if (!pb.raw.empty()) { out.push_back(std::move(pb)); }
                } catch (...) { ++skipped; }

                off = body_at + ph.compressed_page_size;
            }
        }
    }
    return out;
}

size_t compress_all(const std::vector<page_body>& pages, size_t from, size_t to,
                    int level, const ZSTD_CDict* cd) {
    auto* cctx = ZSTD_createCCtx();
    size_t total = 0;
    std::vector<uint8_t> buf;
    for (size_t i = from; i < to; ++i) {
        const auto& p = pages[i].raw;
        buf.resize(ZSTD_compressBound(p.size()));
        size_t r;
        if (cd) {
            r = ZSTD_compress_usingCDict(cctx, buf.data(), buf.size(), p.data(), p.size(), cd);
        } else {
            r = ZSTD_compressCCtx(cctx, buf.data(), buf.size(), p.data(), p.size(), level);
        }
        if (ZSTD_isError(r)) { ZSTD_freeCCtx(cctx); throw std::runtime_error("compress failed"); }
        total += r;
    }
    ZSTD_freeCCtx(cctx);
    return total;
}

// Train over a sample of the given page range. zstd wants roughly 100x the
// dictionary size in samples; more than that mostly costs time.
std::vector<uint8_t> train(const std::vector<page_body>& pages, size_t from, size_t to,
                           size_t dict_size) {
    std::vector<uint8_t> flat;
    std::vector<size_t> sizes;
    const size_t budget = dict_size * 100;
    // Stride through the range so the sample spans all columns rather than the
    // first few chunks -- pages are laid out column by column.
    const size_t n = to - from;
    if (n == 0) { return {}; }
    size_t stride = 1;
    {
        size_t est = 0, cnt = 0;
        for (size_t i = from; i < to && cnt < 512; i += std::max<size_t>(1, n / 512), ++cnt) {
            est += pages[i].raw.size();
        }
        const size_t avg = cnt ? est / cnt : 1;
        const size_t want = avg ? budget / std::max<size_t>(avg, 1) : n;
        if (want && want < n) { stride = n / want; }
        if (!stride) { stride = 1; }
    }
    for (size_t i = from; i < to && flat.size() < budget; i += stride) {
        const auto& p = pages[i].raw;
        if (p.size() < 8) { continue; }
        flat.insert(flat.end(), p.begin(), p.end());
        sizes.push_back(p.size());
    }
    if (sizes.size() < 8) { return {}; }

    std::vector<uint8_t> dict(dict_size);
    auto r = ZDICT_trainFromBuffer(dict.data(), dict.size(), flat.data(), sizes.data(),
                                   unsigned(sizes.size()));
    if (ZDICT_isError(r)) { return {}; }
    dict.resize(r);
    return dict;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr,
            "usage: %s <name> <file.parquet> [dict_bytes=112640] [level=3]\n", argv[0]);
        return 2;
    }
    const std::string name = argv[1];
    const std::string path = argv[2];
    const size_t dict_size = argc > 3 ? size_t(std::stoul(argv[3])) : 112640;
    const int level = argc > 4 ? std::stoi(argv[4]) : 3;

    try {
        auto img = slurp(path);
        auto md = parse_footer(img);

        size_t on_disk_pages = 0, skipped = 0;
        auto pages = collect_pages(img, md, on_disk_pages, skipped);
        if (pages.empty()) { std::fprintf(stderr, "no pages decoded\n"); return 1; }

        const size_t uncompressed = std::accumulate(pages.begin(), pages.end(), size_t(0),
                [] (size_t a, const page_body& p) { return a + p.raw.size(); });
        // Everything that is not page payload: footer, page headers, levels.
        const size_t overhead = img.size() - on_disk_pages;

        const size_t plain = compress_all(pages, 0, pages.size(), level, nullptr);

        // Self-trained: same bytes train and measure, matching what Scylla's
        // autotrainer does for a table's own sstables.
        size_t self = 0;
        auto d_self = train(pages, 0, pages.size(), dict_size);
        if (!d_self.empty()) {
            auto* cd = ZSTD_createCDict(d_self.data(), d_self.size(), level);
            self = compress_all(pages, 0, pages.size(), level, cd);
            ZSTD_freeCDict(cd);
        }

        // Held out: train on the first half, measure the second.
        const size_t mid = pages.size() / 2;
        size_t held_plain = compress_all(pages, mid, pages.size(), level, nullptr);
        size_t held_dict = 0;
        auto d_held = train(pages, 0, mid, dict_size);
        if (!d_held.empty()) {
            auto* cd = ZSTD_createCDict(d_held.data(), d_held.size(), level);
            held_dict = compress_all(pages, mid, pages.size(), level, cd);
            ZSTD_freeCDict(cd);
        }

        // Control: the same bytes, but cut into SSTable-sized chunks and each
        // compressed independently. If the mechanism behind "dictionaries help
        // SSTables but not Parquet" is block size rather than anything about the
        // data, the dictionary should start paying here and only here.
        size_t chunk_plain = 0, chunk_dict = 0;
        const size_t CHUNK = 4096;
        {
            std::vector<page_body> chunks;
            for (const auto& p : pages) {
                for (size_t o = 0; o < p.raw.size(); o += CHUNK) {
                    page_body cb;
                    const size_t n = std::min(CHUNK, p.raw.size() - o);
                    cb.raw.assign(p.raw.begin() + long(o), p.raw.begin() + long(o + n));
                    chunks.push_back(std::move(cb));
                }
            }
            chunk_plain = compress_all(chunks, 0, chunks.size(), level, nullptr);
            auto dc = train(chunks, 0, chunks.size(), dict_size);
            if (!dc.empty()) {
                auto* cd = ZSTD_createCDict(dc.data(), dc.size(), level);
                chunk_dict = compress_all(chunks, 0, chunks.size(), level, cd);
                ZSTD_freeCDict(cd);
            }
        }

        auto pct = [] (size_t a, size_t b) { return b ? 100.0 * double(a) / double(b) : 0.0; };
        std::printf("%s,%zu,%zu,%zu,%zu,%zu,%zu,%zu,%zu,%.2f,%.2f,%zu,%zu,%zu,%zu,%.2f\n",
                    name.c_str(), img.size(), pages.size(), uncompressed, overhead,
                    plain, self, held_plain, held_dict,
                    self ? 100.0 - pct(self, plain) : 0.0,
                    held_dict ? 100.0 - pct(held_dict, held_plain) : 0.0,
                    d_self.size(), skipped,
                    chunk_plain, chunk_dict,
                    chunk_dict ? 100.0 - pct(chunk_dict, chunk_plain) : 0.0);
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
}
