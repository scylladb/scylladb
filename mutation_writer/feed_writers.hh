/*
 * Copyright (C) 2020 ScyllaDB
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

#pragma once

#include "flat_mutation_reader.hh"

namespace mutation_writer {
using reader_consumer = noncopyable_function<future<> (flat_mutation_reader)>;

template <typename Writer>
requires MutationFragmentConsumer<Writer, future<>>
future<> feed_writer(flat_mutation_reader&& rd, Writer&& wr) {
    return do_with(std::move(rd), std::move(wr), [] (flat_mutation_reader& rd, Writer& wr) {
        return rd.fill_buffer(db::no_timeout).then([&rd, &wr] {
            return do_until([&rd] { return rd.is_buffer_empty() && rd.is_end_of_stream(); }, [&rd, &wr] {
                auto f1 = rd.pop_mutation_fragment().consume(wr);
                auto f2 = rd.is_buffer_empty() ? rd.fill_buffer(db::no_timeout) : make_ready_future<>();
                return when_all_succeed(std::move(f1), std::move(f2)).discard_result();
            });
        }).finally([&wr] {
            return wr.consume_end_of_stream();
        });
    });
}

}
