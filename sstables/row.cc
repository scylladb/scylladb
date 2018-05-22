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

future<> sstable::data_consume_rows_at_once(const schema& s, row_consumer& consumer,
        uint64_t start, uint64_t end) {
    return data_read(start, end - start, consumer.io_priority()).then([this, &consumer, &s]
                                               (temporary_buffer<char> buf) {
        shared_sstable sst = shared_from_this();
        data_consume_rows_context ctx(s, sst, consumer, input_stream<char>(), 0, -1);
        ctx.process(buf);
        ctx.verify_end_state();
    });
}

}
