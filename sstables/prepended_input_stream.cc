/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "prepended_input_stream.hh"

using namespace seastar;

class prepended_data_source_impl : public data_source_impl {
private:
    temporary_buffer<char> _buf;
    data_source _ds;
public:
    prepended_data_source_impl(temporary_buffer<char>&& buf, data_source&& ds)
        : _buf(std::move(buf)), _ds(std::move(ds))
    {}

    prepended_data_source_impl(prepended_data_source_impl&&) = default;
    prepended_data_source_impl& operator=(prepended_data_source_impl&&) = default;

    virtual future<temporary_buffer<char>> get() override {
        if (_buf) {
            return make_ready_future<temporary_buffer<char>>(std::move(_buf));
        }
        return _ds.get();
    }
    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        if (n < _buf.size()) {
            _buf.trim_front(n);
            return make_ready_future<temporary_buffer<char>>(std::move(_buf));
        } else {
            n -= _buf.size();
            _buf = {};
            if (n > 0) {
                return _ds.skip(n);
            } else {
                return _ds.get();
            }
        }
    }
    virtual future<> close() override {
        return _ds.close();
    }
};

input_stream<char> make_prepended_input_stream(temporary_buffer<char>&& buf, data_source&& ds) {
    auto impl = std::make_unique<prepended_data_source_impl>(std::move(buf), std::move(ds));
    return input_stream<char>{data_source{std::move(impl)}};
}
