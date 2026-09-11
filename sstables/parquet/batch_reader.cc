/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/batch_reader.hh"
#include "sstables/parquet/format/encryption.hh"
#include "sstables/parquet/writer_impl.hh"
#include "sstables/sstables.hh"
#include "schema/schema.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>

#include <cstring>
#include <limits>
#include <stdexcept>

namespace sstables::parquet {

namespace {

class pq_batch_reader final : public batch_reader {
    shared_sstable _sst;
    schema_ptr _schema;
    reader_permit _permit;
    std::optional<projection> _projection;
    // One byte per leaf, non-zero meaning "not wanted". Empty when reading everything.
    std::vector<uint8_t> _skip;
    // Bytes actually read, so a projection's saving is observable rather than asserted.
    uint64_t _bytes_read = 0;

    bool _init = false;
    format::file_metadata _md;
    std::vector<cql_column> _cols;
    mapped_schema _ms;
    // Cumulative first row of each group, so a batch can say where it starts without the consumer
    // re-deriving it.
    std::vector<int64_t> _rg_start;
    size_t _next_rg = 0;

public:
    pq_batch_reader(shared_sstable sst, schema_ptr s, reader_permit permit,
                    std::optional<projection> proj)
        : _sst(std::move(sst)), _schema(std::move(s)), _permit(std::move(permit))
        , _projection(std::move(proj)) {}

    uint64_t bytes_read() const override { return _bytes_read; }

    const mapped_schema& schema_mapping() const override { return _ms; }
    const std::vector<cql_column>& columns() const override { return _cols; }

    future<> close() override { return make_ready_future<>(); }

    future<> init() override {
        if (_init) { return make_ready_future<>(); }
        return do_init();
    }

    future<std::optional<column_batch>> next() override {
        if (!_init) { co_await do_init(); }
        if (_next_rg >= _md.row_groups.size()) { co_return std::nullopt; }
        const size_t rg = _next_rg++;
        const auto& g = _md.row_groups[rg];

        // One extent per wanted column chunk, rather than one spanning the whole group.
        //
        // Reading the group whole is the right shape when every column is wanted -- the chunks are
        // contiguous, so it is one sequential read. It is the wrong shape for a projection: a
        // column's chunk is its own extent, so skipping a column can skip its bytes, and that is
        // the entire point of a columnar format. Unwanted leaves are marked `absent`, which is how
        // decode_columns() is told not to look at them.
        std::vector<format::column_input> in(g.columns.size());
        struct want_extent { size_t col; uint64_t off; size_t len; };
        std::vector<want_extent> want;
        want.reserve(g.columns.size());
        for (size_t c = 0; c < g.columns.size(); ++c) {
            if (!g.columns[c].meta) {
                throw std::runtime_error("pq batch: column chunk without metadata");
            }
            if (c < _skip.size() && _skip[c]) {
                in[c].absent = true;
                continue;
            }
            const auto& cm = *g.columns[c].meta;
            const int64_t start = cm.dictionary_page_offset ? *cm.dictionary_page_offset
                                                            : cm.data_page_offset;
            if (cm.total_compressed_size <= 0) {
                in[c].absent = true;
                continue;
            }
            want.push_back({c, uint64_t(start), size_t(cm.total_compressed_size)});
        }
        if (want.empty()) { co_return std::nullopt; }

        std::vector<future<temporary_buffer<char>>> fs;
        fs.reserve(want.size());
        for (const auto& e : want) { fs.push_back(_sst->data_read(e.off, e.len, _permit)); }
        auto held = co_await when_all_succeed(fs.begin(), fs.end());
        for (size_t i = 0; i < want.size(); ++i) {
            _bytes_read += held[i].size();
            in[want[i].col].pages = std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(held[i].get()), held[i].size());
            in[want[i].col].first_row = 0;
            in[want[i].col].pages_file_offset = int64_t(want[i].off);
        }

        column_batch out;
        out.first_row = _rg_start[rg];
        out.rows = g.num_rows;
        out.columns = format::decode_columns(in, _md, rg, 0, g.num_rows);
        co_return std::move(out);
    }

private:
    future<> do_init() {
        _init = true;
        if (_sst->get_version() != sstable_version_types::pq) {
            throw std::runtime_error("pq batch: not a parquet sstable");
        }
        const uint64_t len = _sst->ondisk_data_size();
        if (len < 12) { throw std::runtime_error("pq batch: file too short for a footer"); }

        auto tail = co_await _sst->data_read(len - 8, 8, _permit);
        uint32_t flen;
        std::memcpy(&flen, tail.get(), 4);
        if (uint64_t(flen) + 12 > len) { throw std::runtime_error("pq batch: bad footer length"); }
        if (std::memcmp(tail.get() + 4, format::magic_encrypted, 4) == 0) {
            // Refused rather than attempted. Reading an encrypted file needs the key provider, the
            // per-column key resolution and the AAD construction that pq_reader does; a batch
            // reader that quietly returned the ciphertext would be worse than one that does not
            // open the file at all.
            throw std::runtime_error(
                    "pq batch: file has an encrypted footer; the batch reader does not support "
                    "encryption yet");
        }

        auto raw = co_await _sst->data_read(len - 8 - flen, flen, _permit);
        auto blob = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(raw.get()), raw.size());
        // Eager: this reader decodes whole row groups in order, so every group's column metadata
        // is wanted. The lazy mode pq_reader uses exists to keep a *point* read's footer cost
        // independent of file size, which is not this reader's problem.
        _md = format::parse_file_metadata(blob, {}, format::semantic_check::yes,
                                          format::metadata_mode::eager);

        _cols = columns_of(*_schema);
        _ms = recover_mapped_schema(_md, _cols);

        if (_projection) {
            _skip = projection_skip_mask(_ms, _projection->want_regular);
        }

        _rg_start.resize(_md.row_groups.size() + 1, 0);
        for (size_t i = 0; i < _md.row_groups.size(); ++i) {
            _rg_start[i + 1] = _rg_start[i] + _md.row_groups[i].num_rows;
        }
        co_return;
    }
};

} // namespace

std::unique_ptr<batch_reader> make_batch_reader(shared_sstable sst, schema_ptr s,
                                                reader_permit permit,
                                                std::optional<projection> proj) {
    return std::make_unique<pq_batch_reader>(std::move(sst), std::move(s), std::move(permit),
                                            std::move(proj));
}

} // namespace sstables::parquet
