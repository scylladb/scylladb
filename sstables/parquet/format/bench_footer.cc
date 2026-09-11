/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Splits the cost of a cold pq footer into the part a side index could remove and the part it
// could not. Written for docs/dev/parquet-storage-format.md section 10.27 / the row-group side index
// design, which turns on exactly this split.
//
// ---------------------------------------------------------------------------------------------
// READ THIS FIRST: two of the numbers below do not transfer to the node, and 10.27 says which.
//
//   * **parse timings are build-dependent and this is a plain `-O2` build, not Scylla's.** In this
//     harness the lazy walk of a 555 kB footer costs 4.60 us per row group and the *eager* parse
//     that decodes the same bytes into objects costs 3.17 -- skipping more expensive than parsing,
//     which is a genuine defect of the recursive `skip()`. Inlining primitive skips makes it 2.2x
//     faster here and **8 % faster on the node**, moving a real first read by 1.6 %. The change was
//     therefore reverted (10.27). Do not conclude from a ratio here that the node will see it.
//   * **the fetch figures bracket rather than measure.** Scylla's sstable reads bypass the page
//     cache, so the warm number is a floor that no node read enjoys and the `fadvise` number is an
//     upper bound on what advisory eviction proves. The node's own per-miss log (`PQ_READER_PROFILE=1`,
//     `pq_reader - footer miss: ... fetch=...us parse=...us`) is the authority for both phases; use
//     that for anything load-bearing.
//
// What this harness *is* good for, and what 10.27 actually used it for: the decrypt throughput,
// which the node cannot easily isolate, and the footer-versus-index size arithmetic.
// ---------------------------------------------------------------------------------------------
//
// Why it exists at all. `~/pq-lab/footer_share.py` measures what a genuine first read of an sstable
// costs end to end, by defeating the footer cache -- a latency, on a shared box, through the whole
// CQL stack. Neither it nor the per-miss log can price the *decrypt* separately, and the two
// candidate costs want opposite conclusions:
//
//   * **fetch** -- pulling `1 420 B x row groups` off disk. A plaintext file could read a 20 B/group
//     side index instead and skip this. An *encrypted* (PARE) file could not: the side index's
//     extents are offsets into the plaintext footer, so the whole ciphertext must still be fetched
//     and decrypted before any slab can be cut out of it.
//   * **parse** -- the lazy Thrift walk over the row-group list, which is O(all groups) even though
//     it decodes no column metadata, because the list is delta-encoded and variable-length. A side
//     index removes this for plaintext *and* encrypted files alike.
//
// So if the cost is nearly all fetch, the index is worth nothing on encrypted tables; if parse is a
// real share, it is worth something there too. That is not a question latency can answer and it is
// not a question to guess at, hence this.
//
// Method: read a real footer off a real file, then time, separately and each as a min over many
// iterations (contention only ever adds time):
//
//   fetch     pread() of the footer bytes, with the page cache dropped between iterations when
//             possible, so it is a disk read rather than a memcpy. Reported both ways, because
//             which one applies depends on whether the node's sstable reads are O_DIRECT, and the
//             warm figure is a hard lower bound either way.
//   parse     parse_file_metadata(..., metadata_mode::lazy) -- what the reader actually calls.
//   eager     parse_file_metadata(..., metadata_mode::eager) -- what it would cost without the
//             lazy mode, kept as the scale reference the lazy mode is already measured against.
//   mat       materialise_row_group() for one group -- the work that survives any index, since a
//             point read must decode its own group's column list however it found it.
//   decrypt   AES-GCM over the footer-sized buffer, i.e. what a PARE file pays on top of fetch and
//             cannot avoid. Only the throughput matters, so it runs over the same byte count.
//
// Usage: bench_footer <file.parquet> [iterations]

#include "parquet_metadata.hh"
#include "encryption.hh"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

using namespace sstables::parquet::format;
using clk = std::chrono::steady_clock;

namespace {

double us_since(clk::time_point t0) {
    return std::chrono::duration<double, std::micro>(clk::now() - t0).count();
}

// min over `n` runs of `f`, in microseconds.
template <typename F>
double best(int n, F&& f) {
    double m = 1e18;
    for (int i = 0; i < n; ++i) {
        auto t0 = clk::now();
        f();
        m = std::min(m, us_since(t0));
    }
    return m;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: bench_footer <file.parquet> [iterations]\n");
        return 2;
    }
    const char* path = argv[1];
    const int iters = argc > 2 ? std::atoi(argv[2]) : 50;

    int fd = ::open(path, O_RDONLY);
    if (fd < 0) { std::perror("open"); return 1; }
    const off_t size = ::lseek(fd, 0, SEEK_END);
    if (size < 12) { std::fprintf(stderr, "file too small\n"); return 1; }

    unsigned char tail[8];
    if (::pread(fd, tail, 8, size - 8) != 8) { std::perror("pread tail"); return 1; }
    uint32_t flen;
    std::memcpy(&flen, tail, 4);
    const bool encrypted = std::memcmp(tail + 4, "PARE", 4) == 0;
    if (uint64_t(flen) + 12 > uint64_t(size)) { std::fprintf(stderr, "bad footer length\n"); return 1; }
    const off_t foff = size - 8 - off_t(flen);

    std::vector<uint8_t> footer(flen);
    if (::pread(fd, footer.data(), flen, foff) != ssize_t(flen)) { std::perror("pread footer"); return 1; }

    // --- fetch, warm: the footer bytes are in the page cache, so this is the floor.
    std::vector<uint8_t> scratch(flen);
    const double fetch_warm = best(iters, [&] {
        if (::pread(fd, scratch.data(), flen, foff) != ssize_t(flen)) { std::abort(); }
    });

    // --- fetch, cold: drop this file's pages between reads. posix_fadvise(DONTNEED) is advisory
    // and needs the pages clean, which they are for a read-only sstable, but it is not guaranteed
    // -- so the cold figure is reported as an upper bound on what it proves, not as a certainty,
    // and the warm figure above stands on its own regardless.
    double fetch_cold = -1;
    {
        double m = 1e18;
        for (int i = 0; i < iters; ++i) {
            ::posix_fadvise(fd, foff, flen, POSIX_FADV_DONTNEED);
            auto t0 = clk::now();
            if (::pread(fd, scratch.data(), flen, foff) != ssize_t(flen)) { std::abort(); }
            m = std::min(m, us_since(t0));
        }
        fetch_cold = m;
    }

    auto blob = std::span<const uint8_t>(footer);
    if (encrypted) {
        std::printf("%s: PARE (encrypted footer) -- parse timings below would need the key, "
                    "so only fetch and decrypt are meaningful here\n", path);
    }

    file_metadata lazy_md;
    double parse_lazy = -1, parse_eager = -1, mat = -1;
    size_t groups = 0, leaves = 0;
    if (!encrypted) {
        auto do_lazy = [&] {
            parse_lazy = best(iters, [&] {
                auto md = parse_file_metadata(blob, {}, semantic_check::yes, metadata_mode::lazy);
                if (md.row_groups.empty()) { std::abort(); }
            });
        };
        auto do_eager = [&] {
            parse_eager = best(iters, [&] {
                auto md = parse_file_metadata(blob, {}, semantic_check::yes, metadata_mode::eager);
                if (md.row_groups.empty()) { std::abort(); }
            });
        };
        // The lazy walk came out *slower* than the eager parse it exists to avoid, which is
        // surprising enough to be a measurement artifact until shown otherwise -- so the order is
        // switchable and the result has to hold both ways to be reported. (It does; 10.27.)
        if (std::getenv("PQ_BENCH_EAGER_FIRST")) { do_eager(); do_lazy(); }
        else                                     { do_lazy(); do_eager(); }
        lazy_md = parse_file_metadata(blob, {}, semantic_check::yes, metadata_mode::lazy);
        groups = lazy_md.row_groups.size();
        leaves = lazy_md.leaf_count();
        // One group, materialised the way the reader does it: into a fresh single-group metadata.
        const size_t mid = groups / 2;
        mat = best(iters * 10, [&] {
            file_metadata one;
            one.schema = lazy_md.schema;
            one.row_groups.assign(1, lazy_md.row_groups[mid]);
            materialise_row_group(one, 0, blob);
            if (one.row_groups[0].columns.empty()) { std::abort(); }
        });
    }

    // --- decrypt, over the same byte count: what a PARE file adds and an index cannot remove.
    double decrypt = -1;
    {
        encryption_key key{std::vector<uint8_t>(32, 0x5a)};
        std::vector<uint8_t> aad{'b', 'e', 'n', 'c', 'h'};
        std::vector<uint8_t> plain(flen, 0x11);
        std::vector<uint8_t> ct;
        encrypt_module(ct, plain, key, aad, cipher::aes_gcm_v1, false);
        decrypt = best(iters, [&] {
            auto pt = decrypt_module(ct, key, aad, nullptr, cipher::aes_gcm_v1, false);
            if (pt.size() != plain.size()) { std::abort(); }
        });
    }

    const double kb = double(flen) / 1024.0;
    std::printf("\n=== footer cost split: %s ===\n", path);
    std::printf("  file %.1f MB   footer %.1f kB (%.1f %% of file)   row groups %zu   leaves %zu\n",
                double(size) / 1e6, kb, 100.0 * double(flen) / double(size), groups, leaves);
    if (groups) {
        std::printf("  %.0f footer bytes per row group\n", double(flen) / double(groups));
    }
    std::printf("  min of %d iterations; min is the estimator because contention only adds time\n\n",
                iters);
    std::printf("  %-26s %10s %14s %14s\n", "phase", "us", "us per rgroup", "MB/s");
    auto line = [&] (const char* n, double us, bool per_rg, bool thru) {
        std::printf("  %-26s %10.1f %14s %14s\n", n, us,
                    per_rg && groups ? (std::to_string(us / double(groups)).substr(0, 6)).c_str() : "-",
                    thru ? (std::to_string(double(flen) / us).substr(0, 6)).c_str() : "-");
    };
    line("fetch (page cache warm)", fetch_warm, false, true);
    line("fetch (fadvise DONTNEED)", fetch_cold, false, true);
    if (!encrypted) {
        line("parse lazy", parse_lazy, true, true);
        line("parse eager", parse_eager, true, true);
        line("materialise one group", mat, false, false);
    }
    line("decrypt, footer-sized", decrypt, false, true);

    if (!encrypted && groups) {
        // What a 20 B/group side index would replace: the fetch of the whole footer plus the lazy
        // walk over every group. What it cannot replace: materialising the one group that is read.
        const double idx_kb = (20.0 * double(groups) + 32.0) / 1024.0;
        std::printf("\n  a 20 B/group side index would be %.1f kB against the footer's %.1f kB "
                    "(%.0fx smaller)\n", idx_kb, kb, kb / idx_kb);
        std::printf("  what it would remove: the whole fetch and the whole lazy walk for a "
                    "plaintext file;\n");
        std::printf("                        the walk only for a PARE file, whose ciphertext must "
                    "still be fetched\n");
        std::printf("                        and decrypted (%.1f us here) before a slab can be cut "
                    "from it.\n", decrypt);
        std::printf("  what survives it:     materialising the one group that is read, %.1f us.\n",
                    mat);
        std::printf("  For the *sizes* of those phases on a real node, use the per-miss log, not "
                    "the rows above.\n");
    }
    ::close(fd);
    return 0;
}
