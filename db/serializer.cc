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

#include "serializer.hh"
#include "database.hh"
#include "types.hh"
#include "utils/serialization.hh"

typedef uint32_t count_type; // Me thinks 32-bits are enough for "normal" count purposes.

template<>
db::serializer<utils::UUID>::serializer(const utils::UUID& uuid)
        : _item(uuid), _size(2 * sizeof(uint64_t)) {
}

template<>
void db::serializer<utils::UUID>::write(output& out,
        const type& t) {
    out.write(t.get_most_significant_bits());
    out.write(t.get_least_significant_bits());
}

template<>
void db::serializer<utils::UUID>::read(utils::UUID& uuid, input& in) {
    uuid = read(in);
}

template<>
void db::serializer<utils::UUID>::skip(input& in) {
    in.skip(2 * sizeof(uint64_t));
}

template<> utils::UUID db::serializer<utils::UUID>::read(input& in) {
    auto msb = in.read<uint64_t>();
    auto lsb = in.read<uint64_t>();
    return utils::UUID(msb, lsb);
}

template<>
db::serializer<bytes>::serializer(const bytes& b)
        : _item(b), _size(output::serialized_size(b)) {
}

template<>
void db::serializer<bytes>::write(output& out, const type& t) {
    out.write(t);
}

template<>
void db::serializer<bytes>::read(bytes& b, input& in) {
    b = in.read<bytes>();
}

template<>
db::serializer<bytes_view>::serializer(const bytes_view& v)
        : _item(v), _size(output::serialized_size(v)) {
}

template<>
void db::serializer<bytes_view>::write(output& out, const type& t) {
    out.write(t);
}

template<>
void db::serializer<bytes_view>::read(bytes_view& v, input& in) {
    v = in.read<bytes_view>();
}

template<>
bytes_view db::serializer<bytes_view>::read(input& in) {
    return in.read<bytes_view>();
}

template<>
db::serializer<sstring>::serializer(const sstring& s)
        : _item(s), _size(output::serialized_size(s)) {
}

template<>
void db::serializer<sstring>::write(output& out, const type& t) {
    out.write(t);
}

template<>
void db::serializer<sstring>::read(sstring& s, input& in) {
    s = in.read<sstring>();
}

template<>
db::serializer<tombstone>::serializer(const tombstone& t)
        : _item(t), _size(sizeof(t.timestamp) + sizeof(decltype(t.deletion_time.time_since_epoch().count()))) {
}

template<>
void db::serializer<tombstone>::write(output& out, const type& t) {
    out.write(t.timestamp);
    out.write(t.deletion_time.time_since_epoch().count());
}

template<>
void db::serializer<tombstone>::read(tombstone& t, input& in) {
    t.timestamp = in.read<decltype(t.timestamp)>();
    auto deletion_time = in.read<decltype(t.deletion_time.time_since_epoch().count())>();
    t.deletion_time = gc_clock::time_point(gc_clock::duration(deletion_time));
}

template<>
db::serializer<atomic_cell_view>::serializer(const atomic_cell_view& c)
        : _item(c), _size(bytes_view_serializer(c.serialize()).size()) {
}

template<>
void db::serializer<atomic_cell_view>::write(output& out, const atomic_cell_view& t) {
    bytes_view_serializer::write(out, t.serialize());
}

template<>
void db::serializer<atomic_cell_view>::read(atomic_cell_view& c, input& in) {
    c = atomic_cell_view::from_bytes(bytes_view_serializer::read(in));
}

template<>
atomic_cell_view db::serializer<atomic_cell_view>::read(input& in) {
    return atomic_cell_view::from_bytes(bytes_view_serializer::read(in));
}

template<>
db::serializer<collection_mutation_view>::serializer(const collection_mutation_view& c)
        : _item(c), _size(bytes_view_serializer(c.serialize()).size()) {
}

template<>
void db::serializer<collection_mutation_view>::write(output& out, const collection_mutation_view& t) {
    bytes_view_serializer::write(out, t.serialize());
}

template<>
void db::serializer<collection_mutation_view>::read(collection_mutation_view& c, input& in) {
    c = collection_mutation_view::from_bytes(bytes_view_serializer::read(in));
}

template<>
db::serializer<partition_key_view>::serializer(const partition_key_view& key)
    : _item(key), _size(sizeof(uint16_t) /* size */ + key.representation().size()) {
}

template<>
void db::serializer<partition_key_view>::write(output& out, const partition_key_view& key) {
    bytes_view v = key.representation();
    out.write<uint16_t>(v.size());
    out.write(v.begin(), v.end());
}

template<>
void db::serializer<partition_key_view>::read(partition_key_view& b, input& in) {
    auto len = in.read<uint16_t>();
    b = partition_key_view::from_bytes(in.read_view(len));
}

template<>
partition_key_view db::serializer<partition_key_view>::read(input& in) {
    auto len = in.read<uint16_t>();
    return partition_key_view::from_bytes(in.read_view(len));
}

template<>
void db::serializer<partition_key_view>::skip(input& in) {
    auto len = in.read<uint16_t>();
    in.skip(len);
}

template<>
db::serializer<clustering_key_view>::serializer(const clustering_key_view& key)
    : _item(key), _size(sizeof(uint16_t) /* size */ + key.representation().size()) {
}

template<>
void db::serializer<clustering_key_view>::write(output& out, const clustering_key_view& key) {
    bytes_view v = key.representation();
    out.write<uint16_t>(v.size());
    out.write(v.begin(), v.end());
}

template<>
void db::serializer<clustering_key_view>::read(clustering_key_view& b, input& in) {
    auto len = in.read<uint16_t>();
    b = clustering_key_view::from_bytes(in.read_view(len));
}

template<>
clustering_key_view db::serializer<clustering_key_view>::read(input& in) {
    auto len = in.read<uint16_t>();
    return clustering_key_view::from_bytes(in.read_view(len));
}

template<>
db::serializer<clustering_key_prefix_view>::serializer(const clustering_key_prefix_view& key)
    : _item(key), _size(sizeof(uint16_t) /* size */ + key.representation().size()) {
}

template<>
void db::serializer<clustering_key_prefix_view>::write(output& out, const clustering_key_prefix_view& key) {
    bytes_view v = key.representation();
    out.write<uint16_t>(v.size());
    out.write(v.begin(), v.end());
}

template<>
void db::serializer<clustering_key_prefix_view>::read(clustering_key_prefix_view& b, input& in) {
    auto len = in.read<uint16_t>();
    b = clustering_key_prefix_view::from_bytes(in.read_view(len));
}

template<>
clustering_key_prefix_view db::serializer<clustering_key_prefix_view>::read(input& in) {
    auto len = in.read<uint16_t>();
    return clustering_key_prefix_view::from_bytes(in.read_view(len));
}

template<>
db::serializer<frozen_mutation>::serializer(const frozen_mutation& mutation)
    : _item(mutation), _size(sizeof(uint32_t) /* size */ + mutation.representation().size()) {
}

template<>
void db::serializer<frozen_mutation>::write(output& out, const frozen_mutation& mutation) {
    bytes_view v = mutation.representation();
    out.write(v);
}

template<>
void db::serializer<frozen_mutation>::read(frozen_mutation& m, input& in) {
    m = read(in);
}

template<>
frozen_mutation db::serializer<frozen_mutation>::read(input& in) {
    return frozen_mutation(bytes_serializer::read(in));
}

template<>
db::serializer<db::replay_position>::serializer(const db::replay_position& rp)
        : _item(rp), _size(sizeof(uint64_t) * 2) {
}

template<>
void db::serializer<db::replay_position>::write(output& out, const db::replay_position& rp) {
    out.write<uint64_t>(rp.id);
    out.write<uint64_t>(rp.pos);
}

template<>
void db::serializer<db::replay_position>::read(db::replay_position& rp, input& in) {
    rp.id = in.read<uint64_t>();
    rp.pos = in.read<uint64_t>();
}

template class db::serializer<tombstone> ;
template class db::serializer<bytes> ;
template class db::serializer<bytes_view> ;
template class db::serializer<sstring> ;
template class db::serializer<atomic_cell_view> ;
template class db::serializer<collection_mutation_view> ;
template class db::serializer<utils::UUID> ;
template class db::serializer<partition_key_view> ;
template class db::serializer<clustering_key_view> ;
template class db::serializer<clustering_key_prefix_view> ;
template class db::serializer<frozen_mutation> ;
template class db::serializer<db::replay_position> ;
