/*
 * Copyright (C) 2017 ScyllaDB
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

#include "integrity_checked_file_impl.hh"
#include <boost/algorithm/cxx11/all_of.hpp>

namespace sstables {

integrity_checked_file_impl::integrity_checked_file_impl(sstring fname, file f)
        : _fname(std::move(fname)), _file(f) {
    _memory_dma_alignment = f.memory_dma_alignment();
    _disk_read_dma_alignment = f.disk_read_dma_alignment();
    _disk_write_dma_alignment = f.disk_write_dma_alignment();
}

static bytes data_sample(const int8_t* buf, size_t buf_len, size_t sample_off, size_t sample_len) {
    auto begin = buf + sample_off;
    auto len = std::min(sample_len, buf_len - sample_off);

    return bytes(begin, len);
}

static sstring report_zeroed_4k_aligned_blocks(const temporary_buffer<int8_t>& buf) {
    sstring report;
    auto nr_blocks = buf.size() / 4096UL;
    for (auto i = 0UL; i < nr_blocks; i++) {
        auto off = i * 4096UL;
        auto len = std::min(buf.size() - off, 4096UL);

        auto begin = buf.get() + off;
        auto end = begin + len;
        if (boost::algorithm::all_of_equal(begin, end, 0)) {
            report += sprint("%ld, ", off);
        }
    }
    return report;
}

future<size_t>
integrity_checked_file_impl::write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) {
    auto wbuf = temporary_buffer<int8_t>(static_cast<const int8_t*>(buffer), len);

    auto ret = report_zeroed_4k_aligned_blocks(wbuf);
    if (!ret.empty()) {
        sstlog.warn("integrity check warning for {}, stage: before write started, write: {} bytes to offset {}, " \
            "reason: 4k block(s) zeroed, follow their offsets: {}", _fname, len, pos, ret);
    }

    return get_file_impl(_file)->write_dma(pos, buffer, len, pc)
            .then([this, pos, wbuf = std::move(wbuf), buffer = static_cast<const int8_t*>(buffer), len, &pc] (size_t ret) mutable {
        if (ret != len) {
            sstlog.error("integrity check failed for {}, stage: after write finished, write: {} bytes to offset {}, " \
                "reason: only {} bytes were written.", _fname, len, pos, ret);
        }

        auto wbuf_end = wbuf.get() + wbuf.size();
        auto r = std::mismatch(wbuf.get(), wbuf_end, buffer, buffer + len);
        if (r.first != wbuf_end) {
            auto mismatch_off = r.first - wbuf.get();

            sstlog.error("integrity check failed for {}, stage: after write verification, write: {} bytes to offset {}, " \
                "reason: buffer was modified during write call, mismatch at byte {}:\n" \
                " unmodified sample:\t{}\n" \
                " modified sample:  \t{}",
                _fname, len, pos, mismatch_off,
                data_sample(wbuf.get(), wbuf.size(), mismatch_off, 16), data_sample(buffer, len, mismatch_off, 16));

            return make_ready_future<size_t>(ret);
        }

        return _file.dma_read_exactly<int8_t>(pos, len, pc).then([this, pos, wbuf = std::move(wbuf), len, ret] (auto rbuf) mutable {
            if (rbuf.size() != len) {
                sstlog.error("integrity check failed for {}, stage: read after write finished, write: {} bytes to offset {}, " \
                    "reason: only able to read {} bytes for further verification", _fname, len, pos, rbuf.size());
            }

            auto rbuf_end = rbuf.get() + rbuf.size();
            auto r = std::mismatch(rbuf.get(), rbuf.get() + rbuf.size(), wbuf.get(), wbuf.get() + wbuf.size());
            if (r.first != rbuf_end) {
                auto mismatch_off = r.first - rbuf.get();

                sstlog.error("integrity check failed for {}, stage: read after write verification, write: {} bytes to offset {}, " \
                    "reason: data read from underlying storage isn't the same as written, mismatch at byte {}:\n" \
                    " data written sample:\t{}\n" \
                    " data read sample:   \t{}",
                    _fname, len, pos, mismatch_off,
                    data_sample(wbuf.get(), wbuf.size(), mismatch_off, 16), data_sample(rbuf.get(), rbuf.size(), mismatch_off, 16));
            }
            return make_ready_future<size_t>(ret);
        });
    });
}

future<size_t>
integrity_checked_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) {
    // TODO: check integrity before and after file_impl::write_dma() like write_dma() above.
    return get_file_impl(_file)->write_dma(pos, iov, pc);
}

inline file make_integrity_checked_file(sstring name, file f) {
    return file(::make_shared<integrity_checked_file_impl>(std::move(name), f));
}

inline open_flags adjust_flags_for_integrity_checked_file(open_flags flags) {
    if (static_cast<unsigned int>(flags) & O_WRONLY) {
        flags = open_flags((static_cast<unsigned int>(flags) & ~O_WRONLY) | O_RDWR);
    }
    return flags;
}

future<file>
open_integrity_checked_file_dma(sstring name, open_flags flags, file_open_options options) {
    return open_file_dma(name, adjust_flags_for_integrity_checked_file(flags), options).then([name] (file f) {
        return make_ready_future<file>(make_integrity_checked_file(std::move(name), f));
    });
}

future<file>
open_integrity_checked_file_dma(sstring name, open_flags flags) {
    return open_file_dma(name, adjust_flags_for_integrity_checked_file(flags)).then([name] (file f) {
        return make_ready_future<file>(make_integrity_checked_file(std::move(name), f));
    });
}

}
