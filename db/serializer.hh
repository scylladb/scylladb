/*
 * Copyright 2015 Cloudius Systems
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

#ifndef DB_SERIALIZER_HH_
#define DB_SERIALIZER_HH_

#include "utils/data_input.hh"
#include "utils/data_output.hh"
#include "bytes_ostream.hh"
#include "bytes.hh"
#include "mutation.hh"
#include "keys.hh"
#include "database_fwd.hh"
#include "frozen_mutation.hh"
#include "db/commitlog/replay_position.hh"

namespace db {
/**
 * Serialization objects for various types and using "internal" format. (Not CQL, origin whatnot).
 * The design rationale is that a "serializer" can be instantiated for an object, and will contain
 * the obj + size, and is usable as a functor.
 *
 * Serialization can also be done "explicitly" through the static method "write"
 * (Not using "serialize", because writing "serializer<apa>::serialize" all the time is tiring and redundant)
 * though care should be takes than data will fit of course.
 */
template<typename T>
class serializer {
public:
    typedef T type;
    typedef data_output output;
    typedef data_input input;
    typedef serializer<T> _MyType;

    serializer(const type&);

    // apply to memory, must be at least size() large.
    const _MyType& operator()(output& out) const {
        write(out, _item);
        return *this;
    }

    static void write(output&, const T&);
    static void read(T&, input&);
    static T read(input&);
    static void skip(input& in);

    size_t size() const {
        return _size;
    }

    void write(bytes_ostream& out) const {
        auto buf = out.write_place_holder(_size);
        data_output data_out((char*)buf, _size);
        write(data_out, _item);
    }

    void write(data_output& out) const {
        write(out, _item);
    }
private:
    const T& _item;
    size_t _size;
};

template<> serializer<utils::UUID>::serializer(const utils::UUID &);
template<> void serializer<utils::UUID>::write(output&, const type&);
template<> void serializer<utils::UUID>::read(utils::UUID&, input&);
template<> void serializer<utils::UUID>::skip(input&);
template<> utils::UUID serializer<utils::UUID>::read(input&);

template<> serializer<bytes>::serializer(const bytes &);
template<> void serializer<bytes>::write(output&, const type&);
template<> void serializer<bytes>::read(bytes&, input&);

template<> serializer<bytes_view>::serializer(const bytes_view&);
template<> void serializer<bytes_view>::write(output&, const type&);
template<> void serializer<bytes_view>::read(bytes_view&, input&);
template<> bytes_view serializer<bytes_view>::read(input&);

template<> serializer<sstring>::serializer(const sstring&);
template<> void serializer<sstring>::write(output&, const type&);
template<> void serializer<sstring>::read(sstring&, input&);

template<> serializer<tombstone>::serializer(const tombstone &);
template<> void serializer<tombstone>::write(output&, const type&);
template<> void serializer<tombstone>::read(tombstone&, input&);

template<> serializer<atomic_cell_view>::serializer(const atomic_cell_view &);
template<> void serializer<atomic_cell_view>::write(output&, const type&);
template<> void serializer<atomic_cell_view>::read(atomic_cell_view&, input&);
template<> atomic_cell_view serializer<atomic_cell_view>::read(input&);

template<> serializer<collection_mutation_view>::serializer(const collection_mutation_view &);
template<> void serializer<collection_mutation_view>::write(output&, const type&);
template<> void serializer<collection_mutation_view>::read(collection_mutation_view&, input&);

template<> serializer<frozen_mutation>::serializer(const frozen_mutation &);
template<> void serializer<frozen_mutation>::write(output&, const type&);
template<> void serializer<frozen_mutation>::read(frozen_mutation&, input&);
template<> frozen_mutation serializer<frozen_mutation>::read(input&);

template<> serializer<partition_key_view>::serializer(const partition_key_view &);
template<> void serializer<partition_key_view>::write(output&, const partition_key_view&);
template<> void serializer<partition_key_view>::read(partition_key_view&, input&);
template<> partition_key_view serializer<partition_key_view>::read(input&);
template<> void serializer<partition_key_view>::skip(input&);

template<> serializer<clustering_key_view>::serializer(const clustering_key_view &);
template<> void serializer<clustering_key_view>::write(output&, const clustering_key_view&);
template<> void serializer<clustering_key_view>::read(clustering_key_view&, input&);
template<> clustering_key_view serializer<clustering_key_view>::read(input&);

template<> serializer<clustering_key_prefix_view>::serializer(const clustering_key_prefix_view &);
template<> void serializer<clustering_key_prefix_view>::write(output&, const clustering_key_prefix_view&);
template<> void serializer<clustering_key_prefix_view>::read(clustering_key_prefix_view&, input&);
template<> clustering_key_prefix_view serializer<clustering_key_prefix_view>::read(input&);

template<> serializer<db::replay_position>::serializer(const db::replay_position&);
template<> void serializer<db::replay_position>::write(output&, const db::replay_position&);
template<> void serializer<db::replay_position>::read(db::replay_position&, input&);

template<typename T>
T serializer<T>::read(input& in) {
    type t;
    read(t, in);
    return t;
}

extern template class serializer<tombstone>;
extern template class serializer<bytes>;
extern template class serializer<bytes_view>;
extern template class serializer<sstring>;
extern template class serializer<utils::UUID>;
extern template class serializer<partition_key_view>;
extern template class serializer<clustering_key_view>;
extern template class serializer<clustering_key_prefix_view>;
extern template class serializer<db::replay_position>;

typedef serializer<tombstone> tombstone_serializer;
typedef serializer<bytes> bytes_serializer; // Compatible with bytes_view_serializer
typedef serializer<bytes_view> bytes_view_serializer; // Compatible with bytes_serializer
typedef serializer<sstring> sstring_serializer;
typedef serializer<atomic_cell_view> atomic_cell_view_serializer;
typedef serializer<collection_mutation_view> collection_mutation_view_serializer;
typedef serializer<utils::UUID> uuid_serializer;
typedef serializer<partition_key_view> partition_key_view_serializer;
typedef serializer<clustering_key_view> clustering_key_view_serializer;
typedef serializer<clustering_key_prefix_view> clustering_key_prefix_view_serializer;
typedef serializer<frozen_mutation> frozen_mutation_serializer;
typedef serializer<db::replay_position> replay_position_serializer;

}

#endif /* DB_SERIALIZER_HH_ */
