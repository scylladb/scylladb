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

#include "limiting_data_source.hh"
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <cstdint>

using namespace seastar;

class limiting_data_source_impl final : public data_source_impl {
    data_source _src;
    seastar::noncopyable_function<size_t()> _limit_generator;
    temporary_buffer<char> _buf;
    future<temporary_buffer<char>> do_get() {
        uint64_t size = std::min(_limit_generator(), _buf.size());
        auto res = _buf.share(0, size);
        _buf.trim_front(size);
        return make_ready_future<temporary_buffer<char>>(std::move(res));
    }
public:
    limiting_data_source_impl(data_source&& src, seastar::noncopyable_function<size_t()>&& limit_generator)
        : _src(std::move(src))
        , _limit_generator(std::move(limit_generator))
    {}

    limiting_data_source_impl(limiting_data_source_impl&&) noexcept = default;
    limiting_data_source_impl& operator=(limiting_data_source_impl&&) noexcept = default;

    virtual future<temporary_buffer<char>> get() override {
        if (_buf.empty()) {
            _buf.release();
            return _src.get().then([this] (auto&& buf) {
                _buf = std::move(buf);
                return do_get();
            });
        }
        return do_get();
    }
    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        if (n < _buf.size()) {
            _buf.trim_front(n);
            return do_get();
        }
        n -= _buf.size();
        _buf.release();
        return _src.skip(n).then([this] (auto&& buf) {
            _buf = std::move(buf);
            return do_get();
        });
    }
};

data_source make_limiting_data_source(data_source&& src, seastar::noncopyable_function<size_t()>&& limit_generator) {
    return data_source{std::make_unique<limiting_data_source_impl>(std::move(src), std::move(limit_generator))};
}
