/*
 * Copyright 2016 ScylaDB
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

#include "bytes_ostream.hh"
#include "serializer.hh"

namespace ser {

// frame represents a place holder for object size which will be known later


struct place_holder {
    bytes_ostream::place_holder<size_type> ph;

    place_holder(bytes_ostream::place_holder<size_type> ph) : ph(ph) { }

    void set(bytes_ostream& out, bytes_ostream::size_type v) {
        auto stream = ph.get_stream();
        serialize(stream, v);
    }
};

struct frame : public place_holder {
    bytes_ostream::size_type offset;

    frame(bytes_ostream::place_holder<size_type> ph, bytes_ostream::size_type offset)
        : place_holder(ph), offset(offset) { }

    void end(bytes_ostream& out) {
        set(out, out.size() - offset);
    }
};

struct vector_position {
    bytes_ostream::position pos;
    size_type count;
};

//empty frame, behave like a place holder, but is used when no place holder is needed
struct empty_frame {
    void end(bytes_ostream&) {}
    empty_frame() = default;
    empty_frame(const frame&){}
};

inline place_holder start_place_holder(bytes_ostream& out) {
    auto size_ph = out.write_place_holder<size_type>();
    return { size_ph};
}

inline frame start_frame(bytes_ostream& out) {
    auto offset = out.size();
    auto size_ph = out.write_place_holder<size_type>();
    return frame { size_ph, offset };
}

template<typename T>
inline void skip(seastar::simple_input_stream& v, boost::type<T>) {
    deserialize(v, boost::type<T>());
}

template<>
inline void skip(seastar::simple_input_stream& v, boost::type<sstring>) {
    v.skip(deserialize(v, boost::type<size_type>()));
}

template<typename T>
inline void skip(seastar::simple_input_stream& v, boost::type<std::vector<T>>) {
    auto ln = deserialize(v, boost::type<size_type>());
    for (size_type i = 0; i < ln; i++) {
        skip(v, boost::type<T>());
    }
}


}

