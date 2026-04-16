/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "limiting_data_source.hh"

using namespace seastar;

future<temporary_buffer<char>> limiting_data_source_impl::do_get() {
    uint64_t size = std::min(_limit, _buf.size());
    auto res = _buf.share(0, size);
    _buf.trim_front(size);
    return make_ready_future<temporary_buffer<char>>(std::move(res));
}

limiting_data_source_impl::limiting_data_source_impl(data_source&& src, size_t limit) : _src(std::move(src)), _limit(limit) {
}

future<temporary_buffer<char>> limiting_data_source_impl::get() {
    if (_buf.empty()) {
        _buf.release();
        return _src.get().then([this](auto&& buf) {
            _buf = std::move(buf);
            return do_get();
        });
    }
    return do_get();
}

future<temporary_buffer<char>> limiting_data_source_impl::skip(uint64_t n) {
    if (n < _buf.size()) {
        _buf.trim_front(n);
        return do_get();
    }
    n -= _buf.size();
    _buf.release();
    return _src.skip(n).then([this](auto&& buf) {
        _buf = std::move(buf);
        return do_get();
    });
}

data_source make_limiting_data_source(data_source&& src, size_t limit) {
    return data_source{std::make_unique<limiting_data_source_impl>(std::move(src), limit)};
}
