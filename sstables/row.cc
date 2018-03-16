/*
 * Copyright (C) 2015 ScyllaDB
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

#include "sstables.hh"
#include "consumer.hh"
#include "data_consume_context.hh"

namespace sstables {

data_consume_context::~data_consume_context() {
    if (_ctx) {
        auto f = _ctx->close();
        f.handle_exception([ctx = std::move(_ctx), sst = std::move(_sst)](auto) {});
    }
};
data_consume_context::data_consume_context(data_consume_context&& o) noexcept = default;
data_consume_context& data_consume_context::operator=(data_consume_context&& o) noexcept = default;

data_consume_context::data_consume_context(shared_sstable sst, row_consumer& consumer, input_stream<char>&& input, uint64_t start, uint64_t maxlen)
    : _sst(std::move(sst)), _ctx(new data_consume_rows_context(consumer, std::move(input), start, maxlen))
{ }
data_consume_context::data_consume_context() = default;
data_consume_context::operator bool() const noexcept {
    return bool(_ctx);
}
future<> data_consume_context::read() {
    return _ctx->consume_input();
}
future<> data_consume_context::fast_forward_to(uint64_t begin, uint64_t end) {
    _ctx->reset(indexable_element::partition);
    return _ctx->fast_forward_to(begin, end);
}
future<> data_consume_context::skip_to(indexable_element el, uint64_t begin) {
    sstlog.trace("data_consume_rows_context {}: skip_to({} -> {}, el={})", _ctx.get(), _ctx->position(), begin, static_cast<int>(el));
    if (begin <= _ctx->position()) {
        return make_ready_future<>();
    }
    _ctx->reset(el);
    return _ctx->skip_to(begin);
}
bool data_consume_context::eof() const {
    return _ctx->eof();
}

const reader_position_tracker& data_consume_context::reader_position() const {
    return _ctx->reader_position();
}

future<> sstable::data_consume_rows_at_once(row_consumer& consumer,
        uint64_t start, uint64_t end) {
    return data_read(start, end - start, consumer.io_priority()).then([&consumer]
                                               (temporary_buffer<char> buf) {
        data_consume_rows_context ctx(consumer, input_stream<char>(), 0, -1);
        ctx.process(buf);
        ctx.verify_end_state();
    });
}

}
