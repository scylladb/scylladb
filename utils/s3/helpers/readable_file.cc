/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "readable_file.hh"

#include <seastar/util/internal/iovec_utils.hh>

namespace s3 {

future<> client::readable_file::maybe_update_stats() {
    if (_stats) {
        return make_ready_future<>();
    }

    return _client->get_object_stats(_object_name).then([this](auto st) {
        _stats = std::move(st);
        return make_ready_future<>();
    });
}

client::readable_file::readable_file(shared_ptr<client> cln, sstring object_name, seastar::abort_source* as)
    : _client(std::move(cln)), _object_name(std::move(object_name)), _as(as) {
}

client::readable_file::readable_file_handle_impl::readable_file_handle_impl(handle h, sstring object_name)
    : _h(std::move(h)), _object_name(std::move(object_name)) {
}

std::unique_ptr<file_handle_impl> client::readable_file::readable_file_handle_impl::clone() const {
    return std::make_unique<readable_file_handle_impl>(_h, _object_name);
}

shared_ptr<file_impl> client::readable_file::readable_file_handle_impl::to_file() && {
    // TODO: cannot traverse abort source across shards.
    return make_shared<readable_file>(std::move(_h).to_client(), std::move(_object_name), nullptr);
}

std::unique_ptr<file_handle_impl> client::readable_file::dup() {
    return std::make_unique<readable_file_handle_impl>(handle(*_client), _object_name);
}

future<uint64_t> client::readable_file::size() {
    return _client->get_object_size(_object_name);
}

future<size_t> client::readable_file::read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) {
    co_await maybe_update_stats();
    if (pos >= _stats->size) {
        co_return 0;
    }

    auto buf = co_await _client->get_object_contiguous(_object_name, range{pos, len}, _as);
    std::copy_n(buf.get(), buf.size(), reinterpret_cast<uint8_t*>(buffer));
    co_return buf.size();
}

future<size_t> client::readable_file::read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) {
    co_await maybe_update_stats();
    if (pos >= _stats->size) {
        co_return 0;
    }

    auto buf = co_await _client->get_object_contiguous(_object_name, range{pos, internal::iovec_len(iov)}, _as);
    uint64_t off = 0;
    for (auto& v : iov) {
        auto sz = std::min(v.iov_len, buf.size() - off);
        if (sz == 0) {
            break;
        }
        std::copy_n(buf.get() + off, sz, reinterpret_cast<uint8_t*>(v.iov_base));
        off += sz;
    }
    co_return off;
}

future<temporary_buffer<uint8_t>> client::readable_file::dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) {
    co_await maybe_update_stats();
    if (offset >= _stats->size) {
        co_return temporary_buffer<uint8_t>();
    }

    auto buf = co_await _client->get_object_contiguous(_object_name, range{offset, range_size}, _as);
    co_return temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(buf.get_write()), buf.size(), buf.release());
}

future<struct stat> client::readable_file::stat() {
    co_await maybe_update_stats();
    struct stat ret{};
    ret.st_nlink = 1;
    ret.st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
    ret.st_size = _stats->size;
    ret.st_blksize = 1 << 10; // huh?
    ret.st_blocks = _stats->size >> 9;
    // objects are immutable on S3, therefore we can use Last-Modified to set both st_mtime and st_ctime
    ret.st_mtime = _stats->last_modified;
    ret.st_ctime = _stats->last_modified;
    co_return ret;
}

} // s3