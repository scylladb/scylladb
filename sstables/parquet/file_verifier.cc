/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/file_verifier.hh"

#include <algorithm>
#include <span>

#include <seastar/core/align.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <fmt/ranges.h>

#include "sstables/checksum_utils.hh"
#include "sstables/exceptions.hh"
#include "sstables/parquet/page_decoder.hh"
#include "sstables/parquet/thrift.hh"

namespace sstables::parquet {

namespace {

template <typename... Args>
[[noreturn]] void malformed(fmt::format_string<Args...> fmt, Args&&... args) {
    throw malformed_sstable_exception(seastar::format("Parquet file is malformed: {}", fmt::format(fmt, std::forward<Args>(args)...)));
}

// The largest page header that the verifier parses. The headers of our
// writer take less than 100 bytes.
constexpr uint64_t max_header_size = 64 << 10;
// The largest read of an OffsetIndex block, as in fetch_policy.
constexpr uint64_t max_request_size = 128 << 10;
// The footer length and the magic bytes after the footer.
constexpr uint64_t trailer_size = 8;

// Reads the file in order, and computes its digest over every byte that
// it consumes.
class sequential_reader {
    input_stream<char>& _in;
    std::optional<reader_permit> _permit;
    // The bytes read and not consumed, in order: _front, then _back.
    temporary_buffer<char> _front;
    temporary_buffer<char> _back;
    uint64_t _position = 0;
    const uint64_t _size;
    uint32_t _digest = crc32_utils::init_checksum();

    future<temporary_buffer<char>> next_input() {
        if (!_back.empty()) {
            co_return std::exchange(_back, {});
        }
        auto buf = co_await _in.read();
        if (buf.empty()) {
            malformed("the file ends before its size of {} bytes", _size);
        }
        co_return buf;
    }
public:
    sequential_reader(input_stream<char>& in, std::optional<reader_permit> permit, uint64_t size)
        : _in(in), _permit(std::move(permit)), _size(size) {}

    uint64_t position() const noexcept { return _position; }
    uint32_t digest() const noexcept { return _digest; }

    // Consumes `n` bytes. Adds them to `*crc` if it is set.
    future<> consume(uint64_t n, uint32_t* crc = nullptr) {
        if (n > _size - _position) {
            malformed("a read of {} bytes at {} goes past the end of the file at {}", n, _position, _size);
        }
        while (n) {
            if (_front.empty()) {
                _front = co_await next_input();
            }
            const size_t k = std::min<uint64_t>(n, _front.size());
            _digest = crc32_utils::checksum(_digest, _front.get(), k);
            if (crc) {
                *crc = crc32_utils::checksum(*crc, _front.get(), k);
            }
            _front.trim_front(k);
            _position += k;
            n -= k;
        }
    }

    // Returns the next min(n, bytes left) bytes, without consuming them.
    // The span is valid until the next call.
    future<std::span<const char>> peek(uint64_t n) {
        n = std::min(n, _size - _position);
        if (_front.size() < n) {
            std::optional<reader_permit::resource_units> units;
            if (_permit) {
                units = co_await _permit->request_memory(n);
            }
            temporary_buffer<char> buf(n);
            if (units) {
                buf = make_tracked_temporary_buffer(std::move(buf), std::move(*units));
            }
            size_t have = _front.size();
            std::copy_n(_front.get(), have, buf.get_write());
            while (have < n) {
                auto in = co_await next_input();
                const size_t k = std::min<uint64_t>(in.size(), n - have);
                std::copy_n(in.get(), k, buf.get_write() + have);
                have += k;
                in.trim_front(k);
                if (!in.empty()) {
                    _back = std::move(in);
                }
            }
            _front = std::move(buf);
        }
        co_return std::span<const char>(_front.get(), n);
    }
};

// What the page headers of a row group add up to. The footer records the
// same totals per chunk: total_uncompressed_size (headers included),
// num_values, and the null count. A page header that is damaged in one of
// these fields changes a total.
struct row_group_totals {
    uint64_t uncompressed_size = 0;
    uint64_t values = 0;
    uint64_t nulls = 0;
    // False when a page or an OffsetIndex block of the row group could not
    // be checked. Its problem is reported, and the totals are not compared.
    bool complete = true;

    bool operator==(const row_group_totals&) const = default;
};

// Sums the chunk totals of each row group of a footer, and compares them
// with what the pages add up to.
class footer_totals_checker final : public file_metadata_visitor {
    const utils::chunked_vector<row_group_totals>& _pages;
    std::function<void(sstring)> _report;
    row_group_totals _current;
public:
    footer_totals_checker(const utils::chunked_vector<row_group_totals>& pages, std::function<void(sstring)> report)
        : _pages(pages), _report(std::move(report)) {}

    void on_column_chunk(size_t, size_t, std::span<const sstring>, column_chunk&& c) override {
        const auto& m = c.meta_data;
        _current.uncompressed_size += m.total_uncompressed_size;
        _current.values += m.num_values;
        _current.nulls += m.stats && m.stats->null_count ? *m.stats->null_count : 0;
    }
    void on_row_group(size_t g, row_group&&) override {
        if (g < _pages.size() && _pages[g].complete && _pages[g] != _current) {
            const auto& p = _pages[g];
            _report(seastar::format("the page headers of row group {} declare {} bytes, {} entries, and {} nulls, the footer says {}, {}, and {}",
                    g, p.uncompressed_size, p.values, p.nulls, _current.uncompressed_size, _current.values, _current.nulls));
        }
        _current = {};
    }
};

class verifier {
    sequential_reader _r;
    file _file;
    const file_summary& _s;
    const verify_options _options;
    std::function<void(sstring)> _on_error;
    uint64_t _errors = 0;
    utils::chunked_vector<row_group_totals> _totals;

    // Adds a page whose header is checked to the totals of row group `g`.
    void add_page(uint32_t g, const page_header& h, uint64_t header_size) {
        auto& t = _totals[g];
        t.uncompressed_size += header_size + h.uncompressed_page_size;
        if (h.data_page_v2) {
            t.values += h.data_page_v2->num_values;
            t.nulls += h.data_page_v2->num_nulls;
        }
    }

    // Checks the totals of every complete row group against the footer.
    // The footer must have passed its checksum, so it is the footer that
    // the summary comes from.
    future<> check_totals() {
        const uint64_t length = _options.file_size - trailer_size - _s.footer_offset();
        auto in = make_file_input_stream(_file, _s.footer_offset(), length, file_input_stream_options{.buffer_size = 128 * 1024, .read_ahead = 1});
        footer_totals_checker checker(_totals, [this] (sstring what) { report(std::move(what)); });
        std::exception_ptr ex;
        try {
            co_await parse_file_metadata(in, length, checker);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await in.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
    }

    void report(sstring what) {
        ++_errors;
        _on_error(std::move(what));
    }

    bool aborted() const noexcept {
        return _options.abort && _options.abort->abort_requested();
    }

    static sstring column_name(const leaf_column& col) {
        return fmt::to_string(fmt::join(col.path, "."));
    }

    // Parses the header of the page at the reader's position, without
    // consuming it. The page has at most `max_size` bytes. Returns the
    // header and its size.
    future<std::pair<page_header, uint64_t>> read_header(uint64_t max_size) {
        auto window = co_await _r.peek(std::min(max_size, max_header_size));
        thrift::decoder d(window);
        auto h = parse_page_header(d);
        co_return std::pair(std::move(h), d.position());
    }

    // Consumes the body of a page whose header is consumed and passed
    // check_page_sizes(), and checks its CRC.
    future<> verify_body(const page_header& h, const leaf_column& col, uint32_t g, std::string_view what) {
        uint32_t crc = crc32_utils::init_checksum();
        co_await _r.consume(h.compressed_page_size, &crc);
        if (crc != uint32_t(*h.crc)) {
            report(seastar::format("{} of column {} in row group {}: page checksum mismatch: computed {:#x}, the header says {:#x}",
                    what, column_name(col), g, crc, uint32_t(*h.crc)));
        }
    }

    // Verifies a page of `size` bytes at the reader's position, which an
    // OffsetIndex locates. `check` checks its header. A problem is reported,
    // and the reader moves to the end of the page.
    future<> verify_located_page(uint64_t size, const leaf_column& col, leaf_index c, uint32_t g, std::string_view what,
            std::function<void(const page_header&)> check) {
        const uint64_t end = _r.position() + size;
        std::optional<std::pair<page_header, uint64_t>> header;
        std::optional<sstring> error;
        try {
            // The window of read_header() ends at the end of the page, so
            // the header fits in the page.
            header = co_await read_header(size);
            check_page_sizes(header->first, size - header->second);
            check_uncompressed_size(c, header->first, _s.row_group(g).max_chunk_uncompressed_size);
            check(header->first);
        } catch (const malformed_sstable_exception& e) {
            error = e.what();
        }
        if (error) {
            _totals[g].complete = false;
            report(seastar::format("{} of column {} in row group {}: {}", what, column_name(col), g, *error));
            co_await _r.consume(end - _r.position());
            co_return;
        }
        add_page(g, header->first, header->second);
        co_await _r.consume(header->second);
        co_await verify_body(header->first, col, g, what);
    }

    // Reads [offset, offset + size) with requests that are aligned to the
    // file's read alignment and are at most max_request_size, as the
    // fetcher does. So the file allocates each buffer at the size of its
    // request, and a tracked file charges that size.
    future<fragmented_temporary_buffer> read_block(uint64_t offset, uint64_t size) {
        const uint64_t alignment = std::max<uint64_t>(_file.disk_read_dma_alignment(), 1);
        const uint64_t request_size = std::max(align_down(max_request_size, alignment), alignment);
        const uint64_t end = offset + size;
        std::vector<temporary_buffer<char>> pieces;
        for (uint64_t o = align_down(offset, alignment); o < end;) {
            const uint64_t next = std::min(align_up(end, alignment), o + request_size);
            auto buf = co_await _file.dma_read_bulk<char>(o, next - o);
            const uint64_t a = std::max(offset, o);
            const uint64_t b = std::min(end, next);
            if (buf.size() < b - o) {
                malformed("a read of {} bytes at {} returned {}", next - o, o, buf.size());
            }
            pieces.push_back(buf.share(a - o, b - a));
            o = next;
        }
        co_return fragmented_temporary_buffer(std::move(pieces), size);
    }

    future<> verify_row_group(uint32_t g) {
        const auto& rg = _s.row_group(g);
        std::optional<row_group_index> idx;
        // The memory of the decoded block, which it holds while it lives.
        std::optional<reader_permit::resource_units> idx_memory;
        std::optional<sstring> error;
        try {
            auto block = co_await read_block(rg.offset_index_offset, rg.offset_index_length);
            if (_options.permit) {
                idx_memory = co_await reader_permit(*_options.permit).request_memory(row_group_index_decode_memory(_s, g));
            }
            idx = co_await decode_row_group_index(_s, g, std::move(block));
        } catch (const malformed_sstable_exception& e) {
            error = e.what();
        }
        if (error) {
            _totals[g].complete = false;
            report(seastar::format("the OffsetIndex block of row group {}: {}", g, *error));
        }
        if (idx) {
            for (leaf_index c = 0; c < _s.columns().size(); ++c) {
                if (!_s.in_place(g, c)) {
                    continue;
                }
                const auto& col = _s.column(c);
                if (_s.has_dictionary(g, c)) {
                    co_await verify_located_page(idx->dictionary_size(c), col, c, g, "the dictionary page", [&] (const page_header& h) {
                        check_dictionary_page_header(col, c, h);
                    });
                }
                const bool all_null = _s.all_null(g, c);
                for (uint32_t j = 0; j < idx->pages(c); ++j) {
                    const auto page = idx->page_at(c, j);
                    const uint64_t rows = idx->page_end_row(c, j) - page.first_row;
                    co_await verify_located_page(page.size, col, c, g, "a data page", [&] (const page_header& h) {
                        check_data_page_header(col, c, h, rows);
                        const auto enc = h.data_page_v2->enc;
                        if ((enc == encoding::rle_dictionary || enc == encoding::plain_dictionary) && !_s.has_dictionary(g, c)) {
                            malformed("a page uses a dictionary, the footer says that the chunk has none");
                        }
                        if (all_null && h.data_page_v2->num_nulls != h.data_page_v2->num_values) {
                            malformed("the footer says that the chunk is entirely null, a page has {} nulls in {} entries",
                                    h.data_page_v2->num_nulls, h.data_page_v2->num_values);
                        }
                    });
                }
                co_await coroutine::maybe_yield();
            }
        }
        // The OffsetIndex block, and the whole region if the block did not
        // decode. The pages tile the region, so the reader is at the block now.
        co_await _r.consume(rg.offset_index_offset + rg.offset_index_length - _r.position());
    }

    // The late chunks lie between the last row group and the footer: for
    // each column in schema order, the chunks of the row groups before the
    // first one in which it is in place. Each chunk is entirely null. Its
    // pages cover the chunk's rows, in any split.
    future<> verify_late_chunks() {
        const uint64_t end = _s.footer_offset();
        std::optional<sstring> error;
        try {
            for (leaf_index c = 0; c < _s.columns().size(); ++c) {
                const auto& col = _s.column(c);
                for (uint32_t g = 0; g < col.first_in_place; ++g) {
                    if (aborted()) {
                        co_return;
                    }
                    for (uint64_t left = _s.row_group_rows(g); left;) {
                        if (_r.position() >= end) {
                            malformed("the late chunks end at {}, before the chunk of column {} in row group {}", end, column_name(col), g);
                        }
                        auto [h, header_size] = co_await read_header(end - _r.position());
                        if (h.compressed_page_size < 0 || uint64_t(h.compressed_page_size) > end - _r.position() - header_size) {
                            malformed("a late page of column {} in row group {} at {} has {} bytes, the footer starts at {}",
                                    column_name(col), g, _r.position(), h.compressed_page_size, end);
                        }
                        check_page_sizes(h, h.compressed_page_size);
                        check_uncompressed_size(c, h, _s.row_group(g).max_chunk_uncompressed_size);
                        const int64_t rows = h.data_page_v2 ? h.data_page_v2->num_rows : 0;
                        check_data_page_header(col, c, h, rows);
                        if (rows <= 0 || uint64_t(rows) > left || h.data_page_v2->num_nulls != h.data_page_v2->num_values) {
                            malformed("a late page of column {} in row group {} has {} rows and {} nulls in {} entries, {} rows of the chunk are left",
                                    column_name(col), g, rows, h.data_page_v2->num_nulls, h.data_page_v2->num_values, left);
                        }
                        add_page(g, h, header_size);
                        co_await _r.consume(header_size);
                        co_await verify_body(h, col, g, "a late page");
                        left -= rows;
                    }
                    co_await coroutine::maybe_yield();
                }
            }
            if (_r.position() != end) {
                malformed("the late chunks end at {}, the footer starts at {}", _r.position(), end);
            }
        } catch (const malformed_sstable_exception& e) {
            error = e.what();
        }
        if (error) {
            // The walk stopped, so the late chunks of some row groups were
            // not added up.
            for (auto& t : _totals) {
                t.complete = false;
            }
            report(seastar::format("the late chunks: {}", *error));
        }
        if (_r.position() < end) {
            co_await _r.consume(end - _r.position());
        }
    }

public:
    verifier(input_stream<char>& in, file f, const file_summary& s, verify_options options, std::function<void(sstring)> on_error)
        : _r(in, options.permit, options.file_size)
        , _file(std::move(f))
        , _s(s)
        , _options(options)
        , _on_error(std::move(on_error))
        , _totals(s.row_groups())
    {}

    future<uint64_t> run() {
        const uint64_t footer_end = _options.file_size - trailer_size;
        if (_options.file_size < magic.size() + trailer_size || _s.footer_offset() > footer_end) {
            malformed("a file of {} bytes cannot hold a footer at {}", _options.file_size, _s.footer_offset());
        }
        auto head = co_await _r.peek(magic.size());
        if (std::string_view(head.data(), head.size()) != magic) {
            report("the file does not start with the Parquet magic bytes");
        }
        co_await _r.consume(magic.size());
        for (uint32_t g = 0; g < _s.row_groups(); ++g) {
            if (aborted()) {
                co_return _errors;
            }
            co_await verify_row_group(g);
        }
        co_await verify_late_chunks();
        if (aborted()) {
            co_return _errors;
        }
        uint32_t crc = crc32_utils::init_checksum();
        co_await _r.consume(footer_end - _s.footer_offset(), &crc);
        const bool footer_ok = crc == _options.footer_crc32;
        if (!footer_ok) {
            report(seastar::format("the footer checksum is {:#x}, expected {:#x}", crc, _options.footer_crc32));
        }
        co_await _r.consume(trailer_size);
        if (footer_ok) {
            co_await check_totals();
        }
        if (_options.digest && _r.digest() != *_options.digest) {
            report(seastar::format("digest mismatch: expected {}, computed {}", *_options.digest, _r.digest()));
        }
        co_return _errors;
    }
};

} // anonymous namespace

future<uint64_t> verify_file(input_stream<char>& in, file f, const file_summary& s, verify_options options,
        std::function<void(sstring)> on_error) {
    // The totals of the row groups.
    std::optional<reader_permit::resource_units> memory;
    if (options.permit) {
        memory = co_await options.permit->request_memory(s.row_groups() * sizeof(row_group_totals));
    }
    verifier v(in, std::move(f), s, options, std::move(on_error));
    co_return co_await v.run();
}

} // namespace sstables::parquet
